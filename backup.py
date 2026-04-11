import os

from kubernetes import client
from kubernetes.client import CoreV1Api
from kubernetes.stream import stream
from kubernetes.watch import watch

from job import BackupJob
from labels import BACKUP_OWNER_LABEL
from postgres import PostgresBackup


class SnapshotInfo:
    def __init__(self, name, size, storage_class):
        self.name = name
        self.size = size
        self.storage_class = storage_class


class ExposedSnapshotPvc:
    def __init__(self, pvc_name, snapshot: SnapshotInfo):
        self.pvc_name = pvc_name
        self.snapshot = snapshot

class Backup:
    def __init__(self, api_client, owner, namespace):
        self.client = api_client
        self.core_v1: CoreV1Api = client.CoreV1Api(api_client)
        self.apps_v1 = client.AppsV1Api(api_client)
        self.custom_api = client.CustomObjectsApi(api_client)
        self.owner = owner
        self.namespace = namespace
        self.jobs = BackupJob(api_client, owner, namespace)



    def exec_in_single_deployment_pod(self, deployment_name, command):
        # 1. Get the Deployment's label selector
        deployment = self.apps_v1.read_namespaced_deployment(name=deployment_name, namespace=self.namespace)
        selector = deployment.spec.selector.match_labels
        selector_str = self.selector_to_query(selector)

        # 2. List pods matching the selector
        pods = self.core_v1.list_namespaced_pod(namespace=self.namespace, label_selector=selector_str)

        if not pods.items:
            print(f"No pods found for deployment: {deployment_name}")
            return None

        # 3. Select only the first pod
        target_pod = pods.items[0].metadata.name
        print(f"Executing command in pod: {target_pod}: {command}")

        # 4. Execute the command
        resp = stream(self.core_v1.connect_get_namespaced_pod_exec,
                      target_pod,
                      self.namespace,
                      command=command,
                      stderr=True, stdin=False,
                      stdout=True, tty=False)

        return resp

    def selector_to_query(self, selector):
        selector_str = ",".join([f"{k}={v}" for k, v in selector.items()])
        return selector_str

    def delete_owned_snapshots(self):
        group = "snapshot.storage.k8s.io"
        version = "v1"
        response = self.custom_api.delete_collection_namespaced_custom_object(group=group,
            version=version,
            namespace=self.namespace,
            plural="volumesnapshots",
            label_selector=f'{BACKUP_OWNER_LABEL}={self.owner}')

        deleted_snapshots = ", ".join([snapshot["metadata"]["name"] for snapshot in response["items"]])

        print(f"Deleted snapshots {deleted_snapshots}")

    def delete_owned_pvcs(self):
        self.core_v1.delete_collection_namespaced_persistent_volume_claim(
            namespace=self.namespace,
            label_selector=f'{BACKUP_OWNER_LABEL}={self.owner}')

    def create_snapshot_and_wait(self, pvc_name, snapshot_class=None):
        snapshot = self.create_snapshot(pvc_name, snapshot_class)
        self.wait_for_snapshot(snapshot)

    def create_snapshots(self, pvc_names: dict[str, str], snapshot_class=None) -> dict[str, SnapshotInfo]:
        return { name : self.create_snapshot(pvc_name, snapshot_class) for name, pvc_name in pvc_names.items() }

    def create_snapshot(self, pvc_name: str, snapshot_class=None) -> SnapshotInfo:
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
            namespace=self.namespace,
            plural="volumesnapshots",
            body=snapshot_spec,
        )

        original_pvc = self.core_v1.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=self.namespace)
        storage_class = original_pvc.spec.storage_class_name

        return SnapshotInfo(snapshot['metadata']['name'], None, storage_class)

    def wait_for_snapshots(self, snapshots: dict[str, SnapshotInfo]):
        for snapshot in snapshots.values():
            self.wait_for_snapshot(snapshot)

    def wait_for_snapshot(self, snapshot: SnapshotInfo):
        group = "snapshot.storage.k8s.io"
        version = "v1"

        status = self.custom_api.get_namespaced_custom_object_status(group=group,
            version=version,
            namespace=self.namespace,
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
                              namespace=self.namespace,
                              plural="volumesnapshots"):
            obj = event['object']

            if "status" in obj:
                status = obj['status']
                ready_to_use = status['readyToUse']

                if ready_to_use:
                    w.stop()

        print(f"Snapshot {snapshot.name} ready")
        snapshot.size = status['restoreSize']

    def expose_snapshots(self, snapshots : dict[str, SnapshotInfo]) -> dict[str, ExposedSnapshotPvc]:
        return { name: self.expose_snapshot(snapshot) for name, snapshot in snapshots.items() }

    def expose_snapshot(self, snapshot) -> ExposedSnapshotPvc:
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
            namespace=self.namespace,
            body=pvc
        )

        return ExposedSnapshotPvc(response.metadata.name, snapshot)

    def run_kopia(self, application: str, scratch_volume: str, cache_volume: str, snapshot_pvcs: dict[str, ExposedSnapshotPvc]):
        image = "kopia/kopia:0.22.3"

        repository = os.environ["REPOSITORY_URL"]
        username = os.environ.get("REPOSITORY_USERNAME", "default")
        password = os.environ.get("REPOSITORY_PASSWORD")
        hostname = os.environ.get("REPOSITORY_HOSTNAME", "default")
        server_fingerprint = os.environ.get("SERVER_FINGERPRINT")

        args = [
            "--disable-file-logging",
            "--no-check-for-updates",
            "--cache-directory", "/cache",
            "--url", f'"{repository}"',
            f"--override-username='{username}'",
            f"--override-hostname='{hostname}'"
        ]

        if server_fingerprint is not None:
            args.append("--server-cert-fingerprint")
            args.append(server_fingerprint)

        args = " ".join(args)

        command = [
            "bash", "-c", f"""
                kopia repository connect server {args}\
                && kopia snapshot create /data --override-source=/k8s/{self.namespace}/{application}/ --no-progress
                """
        ]

        if os.environ.get('SKIP_KOPIA_UPLOAD') == 'true':
            command = ["ls", "/data"]

        # TODO --send-snapshot-report integrate with something like healthcheck.io

        volume_mounts = [
            client.V1VolumeMount(
                name="cache",
                mount_path="/cache"
            )
        ]

        volumes = [
            client.V1Volume(name="cache",
                            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                claim_name=cache_volume)),
        ]

        if scratch_volume is not None:
            volume_mounts.append(
                client.V1VolumeMount(
                    name="scratch",
                    mount_path="/data/scratch",
                    read_only=True
                )
            )

            volumes.append(
                client.V1Volume(name="scratch",
                                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                    claim_name=scratch_volume,
                                    read_only=True))
            )

        for name, expose in snapshot_pvcs.items():
            mount = client.V1VolumeMount(
                name=name,
                mount_path="/data/" + name,
                read_only=True
            )

            source = client.V1PersistentVolumeClaimVolumeSource(claim_name=expose.pvc_name, read_only=True)
            volume = client.V1Volume(name=name, persistent_volume_claim=source)

            volume_mounts.append(mount)
            volumes.append(volume)

        security_context = client.V1PodSecurityContext(
            run_as_group=0,
            run_as_user=0,
            run_as_non_root=False,
            seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault")
        )

        job = self.jobs.create_job_object(f'backup-kopia', image, command, volume_mounts, volumes,
                                     security_context=security_context, env=[
                client.V1EnvVar(name="KOPIA_PASSWORD", value=password),
            ])
        self.jobs.run_job(job)

    def cleanup(self):
        self.delete_owned_pvcs()
        self.delete_owned_snapshots()
        self.jobs.delete_owned_jobs()


class BackupContext:
    def __init__(self, backup: Backup, postgres: PostgresBackup, scratch_volume):
        self.backup = backup
        self.postgres = postgres
        self.scratch_volume = scratch_volume