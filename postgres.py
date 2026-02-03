from kubernetes import client
from kubernetes.client import CoreV1Api

from job import BackupJob


class PostgresBackup:
    def __init__(self, apiClient, owner):
        self.client = apiClient
        self.core_v1: CoreV1Api = client.CoreV1Api(apiClient)
        self.apps_v1 = client.AppsV1Api(apiClient)
        self.custom_api = client.CustomObjectsApi(apiClient)
        self.jobs = BackupJob(apiClient, owner)
        self.owner = owner

    def dump_postgres(self, name, namespace="default", scratch_volume="scratch"):
        group = "postgresql.cnpg.io"
        version = "v1"

        cluster = self.custom_api.get_namespaced_custom_object(
            group=group,
            version=version,
            namespace=namespace,
            plural="clusters",
            name = name,
        )

        image = cluster['status']['image']

        command = [
            "pg_dump", "-Fc", "-f",
            f'/scratch/{name}.dump'
        ]

        volume_mounts = [client.V1VolumeMount(
            name="scratch",
            mount_path="/scratch",
        )]

        volumes = [
            client.V1Volume(name="scratch", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=scratch_volume)),
        ]

        env = [
            client.V1EnvVar(name="PGHOST", value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(key="host", name=name + "-app"))),
            client.V1EnvVar(name="PGPORT", value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(key="port", name=name + "-app"))),
            client.V1EnvVar(name="PGUSER", value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(key="username", name=name + "-app"))),
            client.V1EnvVar(name="PGPASSWORD", value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(key="password", name=name + "-app"))),
            client.V1EnvVar(name="PGDATABASE", value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(key="dbname", name=name + "-app")))
        ]

        job = self.jobs.create_job_object(f'backup-{name}', image, command, volume_mounts, volumes, env)
        self.jobs.run_job(job, namespace)