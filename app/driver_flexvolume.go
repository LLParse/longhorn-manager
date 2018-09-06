package app

import (
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/Sirupsen/logrus"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/rancher/longhorn-manager/types"
)

const (
	DETECT_FLEXVOLUME_NAME   = "discover-flexvolume-dir"
	DETECT_FLEXVOLUME_SCRIPT = `
    find_kubelet_proc() {
      for proc in $(find /proc -maxdepth 1 -type d); do
        if [ ! -f $proc/cmdline ]; then
          continue
        fi
        if [[ "$(cat $proc/cmdline | tr '\000' '\n' | head -n1 | tr '/' '\n' | tail -n1)" == "kubelet" ]]; then
          echo $proc
          return
        fi
      done
    }
    get_flexvolume_path() {
      proc=$(find_kubelet_proc)
      if [ "$proc" != "" ]; then
        path=$(cat $proc/cmdline | tr '\000' '\n' | grep volume-plugin-dir | tr '=' '\n' | tail -n1)
        if [ "$path" == "" ]; then
          echo '/usr/libexec/kubernetes/kubelet-plugins/volume/exec/'
        else
          echo $path
        fi
        return
      fi
      echo 'no kubelet process found, dunno'
    }
    FLEXVOLUME_PATH=$(get_flexvolume_path)
    curl -sSfL "${DISCOVER_FLEXVOLUME_DIR_SERVICE_HOST}:${DISCOVER_FLEXVOLUME_DIR_SERVICE_PORT}/${FLEXVOLUME_PATH}"
  `
)

func createServer(flexvolumeDir *string, serverPort int32) (*http.Server, <-chan struct{}) {
	server := &http.Server{
		Addr: fmt.Sprintf(":%d", serverPort),
	}
	doneChan := make(chan struct{})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		logrus.Infof("Discovered FlexVolume dir path: %s", r.URL.Path)
		*flexvolumeDir = r.URL.Path
		close(doneChan)
	})
	return server, doneChan
}

// deploy service targeting the pod this process runs in
func deployService(kubeClient *clientset.Clientset, namespace string, serverPort int32) (err error) {
	_, err = kubeClient.CoreV1().Services(namespace).Create(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: DETECT_FLEXVOLUME_NAME,
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				v1.ServicePort{
					Name: "longhorn-driver-deployer",
					Port: serverPort,
				},
			},
			Selector: map[string]string{
				"app": "longhorn-driver-deployer",
			},
			Type: v1.ServiceTypeClusterIP,
		},
	})
	return
}

// deploy job that will detect flexvolume dirpath and send results to service
func deployJob(kubeClient *clientset.Clientset, namespace, managerImage string) (err error) {
	privileged := true
	_, err = kubeClient.BatchV1().Jobs(namespace).Create(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: DETECT_FLEXVOLUME_NAME,
		},
		Spec: batchv1.JobSpec{
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: DETECT_FLEXVOLUME_NAME,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						v1.Container{
							Name:    DETECT_FLEXVOLUME_NAME,
							Image:   managerImage,
							Command: []string{"/bin/bash"},
							Args:    []string{"-c", DETECT_FLEXVOLUME_SCRIPT},
							SecurityContext: &v1.SecurityContext{
								Privileged: &privileged,
							},
						},
					},
					RestartPolicy: v1.RestartPolicyNever,
					HostPID:       true,
				},
			},
		},
	})
	return
}

func deployResources(kubeClient *clientset.Clientset, namespace, managerImage string, serverPort int32) {
	var wg sync.WaitGroup
	defer wg.Wait()

	runAsync(&wg, func() {
		if err := deployService(kubeClient, namespace, serverPort); err != nil {
			logrus.Warnf("Failed to deploy flexvolume detection service: %v", err)
		}
	})
	runAsync(&wg, func() {
		if err := deployJob(kubeClient, namespace, managerImage); err != nil {
			logrus.Warnf("Failed to deploy flexvolume detection job: %v", err)
		}
	})
}

func cleanupResources(kubeClient *clientset.Clientset, namespace string, server *http.Server) {
	var wg sync.WaitGroup
	defer wg.Wait()

	runAsync(&wg, func() {
		if err := server.Shutdown(nil); err != nil {
			logrus.Warnf("Failed to shutdown server: %v", err)
		}
	})
	runAsync(&wg, func() {
		if err := kubeClient.CoreV1().Services(namespace).Delete(DETECT_FLEXVOLUME_NAME, nil); err != nil {
			logrus.Warnf("Failed to cleanup flexvolume detection service: %v", err)
		}
	})
	runAsync(&wg, func() {
		deletePolicyForeground := metav1.DeletePropagationBackground
		if err := kubeClient.BatchV1().Jobs(namespace).Delete(DETECT_FLEXVOLUME_NAME, &metav1.DeleteOptions{
			PropagationPolicy: &deletePolicyForeground,
		}); err != nil {
			logrus.Warnf("Failed to cleanup flexvolume detection job: %v", err)
		}
	})
}

func discoverFlexvolumeDir(kubeClient *clientset.Clientset, managerImage string) (dir string, err error) {
	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return "", fmt.Errorf("Cannot detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}
	serverPort := int32(8080)
	flexvolumeDir := ""

	server, doneChan := createServer(&flexvolumeDir, serverPort)
	go server.ListenAndServe()

	deployResources(kubeClient, namespace, managerImage, serverPort)

	<-doneChan

	cleanupResources(kubeClient, namespace, server)
	return flexvolumeDir, nil
}
