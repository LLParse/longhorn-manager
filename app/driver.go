package app

import (
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/urfave/cli"

	pvController "github.com/kubernetes-incubator/external-storage/lib/controller"
	appsv1beta2 "k8s.io/api/apps/v1beta2"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/pkg/util/version"

	longhornclient "github.com/rancher/longhorn-manager/client"
	"github.com/rancher/longhorn-manager/controller"
	"github.com/rancher/longhorn-manager/csi"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

const (
	FlagFlexvolumeDir    = "flexvolume-dir"
	EnvFlexvolumeDir     = "FLEXVOLUME_DIR"
	DefaultFlexvolumeDir = "/usr/libexec/kubernetes/kubelet-plugins/volume/exec/"

	LonghornFlexvolumeDriver = "longhorn-flexvolume-driver"

	FlagManagerURL = "manager-url"

	FlagDriver           = "driver"
	FlagDriverCSI        = "csi"
	FlagDriverFlexvolume = "flexvolume"

	FlagCSIAttacherImage        = "csi-attacher-image"
	FlagCSIProvisionerImage     = "csi-provisioner-image"
	FlagCSIDriverRegistrarImage = "csi-driver-registrar-image"
	FlagCSIProvisionerName      = "csi-provisioner-name"
	EnvCSIAttacherImage         = "CSI_ATTACHER_IMAGE"
	EnvCSIProvisionerImage      = "CSI_PROVISIONER_IMAGE"
	EnvCSIDriverRegistrarImage  = "CSI_DRIVER_REGISTRAR_IMAGE"
	EnvCSIProvisionerName       = "CSI_PROVISIONER_NAME"
)

func DeployDriverCmd() cli.Command {
	return cli.Command{
		Name: "deploy-driver",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  FlagDriver,
				Usage: "Specify the driver, choices are: flexvolume, csi. default option will deploy CSI for Kubernetes v1.10+, Flexvolume for Kubernetes v1.8 and v1.9.",
			},
			cli.StringFlag{
				Name:  FlagManagerImage,
				Usage: "Specify Longhorn manager image",
			},
			cli.StringFlag{
				Name:  FlagManagerURL,
				Usage: "Longhorn manager API URL",
			},
			cli.StringFlag{
				Name:   FlagFlexvolumeDir,
				Usage:  "Specify the location of flexvolume plugin for Kubernetes on the host",
				EnvVar: EnvFlexvolumeDir,
			},
			cli.StringFlag{
				Name:   FlagCSIAttacherImage,
				Usage:  "Specify CSI attacher image",
				EnvVar: EnvCSIAttacherImage,
				Value:  csi.DefaultCSIAttacherImage,
			},
			cli.StringFlag{
				Name:   FlagCSIProvisionerImage,
				Usage:  "Specify CSI provisioner image",
				EnvVar: EnvCSIProvisionerImage,
				Value:  csi.DefaultCSIProvisionerImage,
			},
			cli.StringFlag{
				Name:   FlagCSIDriverRegistrarImage,
				Usage:  "Specify CSI driver-registrar image",
				EnvVar: EnvCSIDriverRegistrarImage,
				Value:  csi.DefaultCSIDriverRegistrarImage,
			},
			cli.StringFlag{
				Name:   FlagCSIProvisionerName,
				Usage:  "Specify CSI provisioner name",
				EnvVar: EnvCSIProvisionerName,
				Value:  csi.DefaultCSIProvisionerName,
			},
		},
		Action: func(c *cli.Context) {
			if err := deployDriver(c); err != nil {
				logrus.Fatalf("Error deploying driver: %v", err)
			}
		},
	}
}

func deployDriver(c *cli.Context) error {
	managerImage := c.String(FlagManagerImage)
	if managerImage == "" {
		return fmt.Errorf("require %v", FlagManagerImage)
	}
	managerURL := c.String(FlagManagerURL)
	if managerURL == "" {
		return fmt.Errorf("require %v", FlagManagerURL)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return errors.Wrap(err, "unable to get client config")
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get k8s client")
	}

	driverDetected := false
	driver := c.String(FlagDriver)
	if driver == "" {
		driverDetected = true
		driver, err = chooseDriver(kubeClient)
		logrus.Debugf("Driver %s will be used after automatic detection", driver)
		if err != nil {
			return err
		}
	} else {
		logrus.Debugf("User specified the driver %s", driver)
	}

	if driver == FlagDriverCSI {
		err := csi.CheckMountPropagationWithNode(managerURL)
		if err != nil {
			logrus.Warnf("Got an error when checking MountPropagation with node status, %v", err)
			if driverDetected {
				logrus.Infof("MountPropagation check failed, fall back to use the Flexvolume")
				driver = FlagDriverFlexvolume
			} else {
				// if user explicitly choose CSI but we cannot deploy.
				// In this case we should error out instead.
				return fmt.Errorf("CSI cannot be deployed because MountPropagation is not set on kubelet and api-server")
			}
		}
	}

	switch driver {
	case FlagDriverCSI:
		logrus.Debug("Deploying CSI driver")
		err = deployCSIDriver(kubeClient, c, managerImage, managerURL)
	case FlagDriverFlexvolume:
		logrus.Debug("Deploying Flexvolume driver")
		err = deployFlexvolumeDriver(kubeClient, c, managerImage, managerURL)
	default:
		return fmt.Errorf("Unsupported driver %s", driver)
	}

	return err
}

// chooseDriver can chose the right driver by k8s server version
// 1.10+ csi
// v1.8/1.9 flexvolume
func chooseDriver(kubeClient *clientset.Clientset) (string, error) {
	serverVersion, err := kubeClient.Discovery().ServerVersion()
	if err != nil {
		return "", errors.Wrap(err, "Cannot choose driver automatically: failed to get Kubernetes server version")
	}
	currentVersion := version.MustParseSemantic(serverVersion.GitVersion)
	minVersion := version.MustParseSemantic(types.CSIKubernetesMinVersion)
	if currentVersion.AtLeast(minVersion) {
		return FlagDriverCSI, nil
	}
	return FlagDriverFlexvolume, nil
}

func deployCSIDriver(kubeClient *clientset.Clientset, c *cli.Context, managerImage, managerURL string) error {
	csiAttacherImage := c.String(FlagCSIAttacherImage)
	csiProvisionerImage := c.String(FlagCSIProvisionerImage)
	csiDriverRegistrarImage := c.String(FlagCSIDriverRegistrarImage)
	csiProvisionerName := c.String(FlagCSIProvisionerName)
	namespace := os.Getenv(types.EnvPodNamespace)
	serviceAccountName := os.Getenv(types.EnvServiceAccount)

	attacherDeployment := csi.NewAttacherDeployment(namespace, serviceAccountName, csiAttacherImage)
	if err := attacherDeployment.Deploy(kubeClient); err != nil {
		return err
	}

	provisionerDeployment := csi.NewProvisionerDeployment(namespace, serviceAccountName, csiProvisionerImage, csiProvisionerName)
	if err := provisionerDeployment.Deploy(kubeClient); err != nil {
		return err
	}

	pluginDeployment := csi.NewPluginDeployment(namespace, serviceAccountName, csiDriverRegistrarImage, managerImage, managerURL)
	if err := pluginDeployment.Deploy(kubeClient); err != nil {
		return err
	}

	defer func() {
		var wg sync.WaitGroup
		notify := func(kubeClient *clientset.Clientset, f func(*clientset.Clientset)) {
			f(kubeClient)
			wg.Done()
		}

		wg.Add(3)
		go notify(kubeClient, attacherDeployment.Cleanup)
		go notify(kubeClient, provisionerDeployment.Cleanup)
		go notify(kubeClient, pluginDeployment.Cleanup)
		wg.Wait()
	}()

	done := make(chan struct{})
	util.RegisterShutdownChannel(done)

	<-done

	return nil
}

func deployFlexvolumeDriver(kubeClient *clientset.Clientset, c *cli.Context, managerImage, managerURL string) error {
	flexvolumeDir := c.String(FlagFlexvolumeDir)
	if flexvolumeDir == "" {
		var err error
		flexvolumeDir, err = discoverFlexvolumeDir(kubeClient)
		if err != nil {
			logrus.Warnf("Failed to detect flexvolume dir, fall back to default: %v", err)
		}
		if flexvolumeDir == "" {
			flexvolumeDir = DefaultFlexvolumeDir
		}
	} else {
		logrus.Infof("User specified Flexvolume dir at: %v", flexvolumeDir)
	}

	dsOps, err := newDaemonSetOps(kubeClient)
	if err != nil {
		return err
	}

	d, err := dsOps.Get(LonghornFlexvolumeDriver)
	if err != nil {
		return err
	}
	if d != nil {
		if err := dsOps.Delete(LonghornFlexvolumeDriver); err != nil {
			return err
		}
	}
	logrus.Infof("Install Flexvolume to Kubernetes nodes directory %v", flexvolumeDir)
	if _, err := dsOps.Create(LonghornFlexvolumeDriver, getFlexvolumeDaemonSetSpec(managerImage, flexvolumeDir)); err != nil {
		return err
	}
	defer func() {
		if err := dsOps.Delete(LonghornFlexvolumeDriver); err != nil {
			logrus.Warnf("Fail to cleanup %v: %v", LonghornFlexvolumeDriver, err)
		}
	}()

	done := make(chan struct{})
	util.RegisterShutdownChannel(done)

	if err = startProvisioner(kubeClient, managerURL, done); err != nil {
		return err
	}

	return nil
}

func getFlexvolumeDaemonSetSpec(image, flexvolumeDir string) *appsv1beta2.DaemonSet {
	cmd := []string{
		"/entrypoint.sh",
	}
	privilege := true
	d := &appsv1beta2.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: LonghornFlexvolumeDriver,
		},
		Spec: appsv1beta2.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": LonghornFlexvolumeDriver,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: LonghornFlexvolumeDriver,
					Labels: map[string]string{
						"app": LonghornFlexvolumeDriver,
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:    LonghornFlexvolumeDriver,
							Image:   image,
							Command: cmd,
							SecurityContext: &v1.SecurityContext{
								Privileged: &privilege,
							},
							ImagePullPolicy: v1.PullAlways,
							Env: []v1.EnvVar{
								{
									Name: "NODE_NAME",
									ValueFrom: &v1.EnvVarSource{
										FieldRef: &v1.ObjectFieldSelector{
											FieldPath: "spec.nodeName",
										},
									},
								},
								{
									Name:  "LONGHORN_BACKEND_SVC",
									Value: "longhorn-backend",
								},
							},
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "flexvolume-longhorn-mount",
									MountPath: "/flexmnt",
								},
								{
									Name:      "usr-local-bin-mount",
									MountPath: "/binmnt",
								},
								{
									Name:      "host-proc-mount",
									MountPath: "/host/proc",
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "flexvolume-longhorn-mount",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: flexvolumeDir,
								},
							},
						},
						{
							Name: "host-proc-mount",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/proc",
								},
							},
						},
						{
							Name: "usr-local-bin-mount",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/usr/local/bin",
								},
							},
						},
					},
				},
			},
		},
	}
	return d
}

func discoverFlexvolumeDir(kubeClient *clientset.Clientset) (dir string, err error) {
	flexvolumeDir := ""

	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return "", fmt.Errorf("Cannot detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}
	serverPort := int32(8080)
	objectMeta := metav1.ObjectMeta{
		Name: "discover-flexvolume-dir",
	}

	// start server to receive results
	doneChan := make(chan struct{})
	s := &http.Server{
		Addr: fmt.Sprintf(":%d", serverPort),
	}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		logrus.Infof("Discovered FlexVolume dir path: %s", r.URL.Path)
		flexvolumeDir = r.URL.Path
		close(doneChan)
	})
	go s.ListenAndServe()

	// before returning, shutdown server and delete k8s resources
	defer func() {
		var wg sync.WaitGroup

		notify := func(f func()) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				f()
			}()
		}
		notify(func() {
			s.Shutdown(nil)
		})
		notify(func() {
			kubeClient.CoreV1().Services(namespace).Delete(objectMeta.Name, nil)
		})
		notify(func() {
			deletePolicyForeground := metav1.DeletePropagationBackground
			kubeClient.BatchV1().Jobs(namespace).Delete(objectMeta.Name, &metav1.DeleteOptions{
				PropagationPolicy: &deletePolicyForeground,
			})
		})

		wg.Wait()
	}()

	// create self-targeting service
	kubeClient.CoreV1().Services(namespace).Create(&v1.Service{
		ObjectMeta: objectMeta,
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

	// create job that will send results to our server
	privilege := true
	_, err = kubeClient.BatchV1().Jobs(namespace).Create(&batchv1.Job{
		ObjectMeta: objectMeta,
		Spec: batchv1.JobSpec{
			Template: v1.PodTemplateSpec{
				ObjectMeta: objectMeta,
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						v1.Container{
							Name:    objectMeta.Name,
							Image:   "busybox",
							Command: []string{"/bin/sh"},
							Args: []string{"-c",
								`
								find_kubelet_proc() {
									for proc in $(find /proc -type d -maxdepth 1); do
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
								wget -q -O - "${DISCOVER_FLEXVOLUME_DIR_SERVICE_HOST}:${DISCOVER_FLEXVOLUME_DIR_SERVICE_PORT}/${FLEXVOLUME_PATH}"
								`},
							SecurityContext: &v1.SecurityContext{
								Privileged: &privilege,
							},
						},
					},
					RestartPolicy: v1.RestartPolicyNever,
					HostPID:       true,
				},
			},
		},
	})

	if err != nil {
		return "", err
	}

	<-doneChan

	return flexvolumeDir, nil
}

func startProvisioner(kubeClient *clientset.Clientset, managerURL string, stopCh <-chan struct{}) error {
	logrus.Debug("Enable the built-in Longhorn provisioner only for FlexVolume")

	clientOpts := &longhornclient.ClientOpts{Url: managerURL}
	apiClient, err := longhornclient.NewRancherClient(clientOpts)
	if err != nil {
		return errors.Wrap(err, "Cannot start Provisioner: failed to initialize Longhorn API client")
	}

	serverVersion, err := kubeClient.Discovery().ServerVersion()
	if err != nil {
		return errors.Wrap(err, "Cannot start Provisioner: failed to get Kubernetes server version")
	}
	provisioner := controller.NewProvisioner(apiClient)
	pc := pvController.NewProvisionController(
		kubeClient,
		controller.LonghornProvisionerName,
		provisioner,
		serverVersion.GitVersion,
	)

	pc.Run(stopCh)
	logrus.Debug("Stop the built-in Longhorn provisioner")
	return nil
}

type DaemonSetOps struct {
	namespace  string
	kubeClient *clientset.Clientset
}

func newDaemonSetOps(kubeClient *clientset.Clientset) (*DaemonSetOps, error) {
	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return nil, fmt.Errorf("Cannot detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}
	return &DaemonSetOps{
		namespace, kubeClient,
	}, nil
}

func (ops *DaemonSetOps) Get(name string) (*appsv1beta2.DaemonSet, error) {
	d, err := ops.kubeClient.AppsV1beta2().DaemonSets(ops.namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return d, nil
}

func (ops *DaemonSetOps) Create(name string, d *appsv1beta2.DaemonSet) (*appsv1beta2.DaemonSet, error) {
	return ops.kubeClient.AppsV1beta2().DaemonSets(ops.namespace).Create(d)
}

func (ops *DaemonSetOps) Delete(name string) error {
	propagation := metav1.DeletePropagationForeground
	return ops.kubeClient.AppsV1beta2().DaemonSets(ops.namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &propagation})
}
