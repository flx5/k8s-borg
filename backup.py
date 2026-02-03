from kubernetes import client
from kubernetes.client import CoreV1Api
from kubernetes.stream import stream
from kubernetes.watch import watch

BACKUP_OWNER_LABEL = "flx5.backup/owner"

class SnapshotInfo:
    def __init__(self, name, size, storage_class):
        self.name = name
        self.size = size
        self.storage_class = storage_class

class Backup:
    def __init__(self, apiClient, owner):
        self.client = apiClient
        self.core_v1: CoreV1Api = client.CoreV1Api(apiClient)
        self.apps_v1 = client.AppsV1Api(apiClient)
        self.custom_api = client.CustomObjectsApi(apiClient)
        self.owner = owner



    def exec_in_single_deployment_pod(self, deployment_name, command, namespace="default"):
        # 1. Get the Deployment's label selector
        deployment = self.apps_v1.read_namespaced_deployment(name=deployment_name, namespace=namespace)
        selector = deployment.spec.selector.match_labels
        selector_str = self.selector_to_query(selector)

        # 2. List pods matching the selector
        pods = self.core_v1.list_namespaced_pod(namespace=namespace, label_selector=selector_str)

        if not pods.items:
            print(f"No pods found for deployment: {deployment_name}")
            return None

        # 3. Select only the first pod
        target_pod = pods.items[0].metadata.name
        print(f"Executing command in pod: {target_pod}: {command}")

        # 4. Execute the command
        resp = stream(self.core_v1.connect_get_namespaced_pod_exec,
                      target_pod,
                      namespace,
                      command=command,
                      stderr=True, stdin=False,
                      stdout=True, tty=False)

        return resp

    def selector_to_query(self, selector):
        selector_str = ",".join([f"{k}={v}" for k, v in selector.items()])
        return selector_str

    def delete_owned_snapshots(self, namespace="default"):
        group = "snapshot.storage.k8s.io"
        version = "v1"
        response = self.custom_api.delete_collection_namespaced_custom_object(group=group,
            version=version,
            namespace=namespace,
            plural="volumesnapshots",
            label_selector=f'{BACKUP_OWNER_LABEL}={self.owner}')

        deleted_snapshots = ", ".join([snapshot["metadata"]["name"] for snapshot in response["items"]])

        print(f"Deleted snapshots {deleted_snapshots}")

    def delete_owned_pvcs(self, namespace="default"):
        response = self.core_v1.delete_collection_namespaced_persistent_volume_claim(
            namespace=namespace,
            label_selector=f'{BACKUP_OWNER_LABEL}={self.owner}')

    def create_snapshot_and_wait(self, pvc_name, namespace="default", snapshot_class=None):
        snapshot = self.create_snapshot(pvc_name, namespace, snapshot_class)
        self.wait_for_snapshot(snapshot, namespace)

    def create_snapshot(self, pvc_name, namespace="default", snapshot_class=None) -> SnapshotInfo:
        group = "snapshot.storage.k8s.io"
        version = "v1"

        snapshot_spec = {
            "apiVersion": group + "/" + version,
            "kind": "VolumeSnapshot",
            "metadata": {
                "generateName": f'{pvc_name}-snap-',
                "labels": {
                    BACKUP_OWNER_LABEL: self.owner
                }
            },
            "spec": {
                "volumeSnapshotClassName": snapshot_class,
                "source": {"persistentVolumeClaimName": pvc_name}
            }
        }

        # create the resource
        snapshot = self.custom_api.create_namespaced_custom_object(
            group=group,
            version=version,
            namespace=namespace,
            plural="volumesnapshots",
            body=snapshot_spec,
        )

        original_pvc = self.core_v1.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace)
        storage_class = original_pvc.spec.storage_class_name

        return SnapshotInfo(snapshot['metadata']['name'], None, storage_class)

    def wait_for_snapshot(self, snapshot: SnapshotInfo, namespace="default"):
        group = "snapshot.storage.k8s.io"
        version = "v1"

        status = self.custom_api.get_namespaced_custom_object_status(group=group,
            version=version,
            namespace=namespace,
            plural="volumesnapshots", name=snapshot.name)

        if "status" in status and status['status']['readyToUse']:
            print(f"Snapshot {snapshot.name} ready")
            snapshot.size = status['status']['restoreSize']
            return

        resource_version = status['metadata']['resourceVersion']

        w = watch.Watch()

        # Stream events for pods in the given namespace
        # Use v1.list_pod_for_all_namespaces for cluster-wide watching
        selector = self.selector_to_query({"metadata.name": snapshot.name})

        print(f"Waiting for snapshot {snapshot.name} to become available")
        for event in w.stream(self.custom_api.list_namespaced_custom_object,
                              resource_version=resource_version,
                              field_selector=selector,
                              group=group,
                              version=version,
                              namespace=namespace,
                              plural="volumesnapshots"):
            obj = event['object']

            if "status" in obj:
                status = obj['status']
                ready_to_use = status['readyToUse']

                if ready_to_use:
                    w.stop()

        print(f"Snapshot {snapshot.name} ready")
        snapshot.size = status['restoreSize']

    def expose_snapshot(self, snapshot, namespace = "default") -> SnapshotInfo:
        # Define the PVC object
        pvc = client.V1PersistentVolumeClaim(
            metadata=client.V1ObjectMeta(
                generate_name=snapshot.name,
                labels={
                    BACKUP_OWNER_LABEL: self.owner
                }
            ),
            spec=client.V1PersistentVolumeClaimSpec(
                # ROX allows the volume to be mounted as read-only by many nodes
                access_modes=["ReadOnlyMany"],
                resources=client.V1ResourceRequirements(
                    requests={"storage": snapshot.size},
                ),
                storage_class_name=snapshot.storage_class,
                data_source=client.V1TypedLocalObjectReference(
                    api_group="snapshot.storage.k8s.io",
                    kind="VolumeSnapshot",
                    name=snapshot.name
                )
            )
        )

        response = self.core_v1.create_namespaced_persistent_volume_claim(
            namespace=namespace,
            body=pvc
        )

        return response.metadata.name