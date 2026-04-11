from kubernetes import client, config

from backup import Backup
from job import BackupJob
from postgres import PostgresBackup

# TODO Check for append only acl: https://github.com/kopia/kopia/issues/1982#issuecomment-3253507634
# TODO Disable swap on k8s nodes!

# Configs can be set in Configuration class directly or using helper utility
configuration = config.load_kube_config()

# Enter a context with an instance of the API kubernetes.client
with client.ApiClient(configuration) as api_client:
    owner = "testbackup"
    namespace = "nextcloud"
    scratch_volume = "nextcloud-scratch"
    application = "nextcloud"

    backup = Backup(api_client, owner)
    postgres = PostgresBackup(api_client, owner)
    jobs = BackupJob(api_client, owner)

    backup.exec_in_single_deployment_pod("nextcloud",
                                                    ["./occ", "maintenance:mode", "--on"], namespace)

    try:
        data_snapshot = backup.create_snapshot("nextcloud-data-new", namespace)
        app_snapshot = backup.create_snapshot("nextcloud-app-new", namespace)
        postgres.dump_postgres("pg-nextcloud", "nextcloud", scratch_volume=scratch_volume)

        backup.wait_for_snapshot(data_snapshot, namespace)
        backup.wait_for_snapshot(app_snapshot, namespace)
    finally:
        backup.exec_in_single_deployment_pod("nextcloud",
                                             ["./occ", "maintenance:mode", "--off"], namespace)

    app_expose = backup.expose_snapshot(app_snapshot, namespace)
    data_expose = backup.expose_snapshot(data_snapshot, namespace)

    image = "kopia/kopia:0.22.3"


     # kopia snapshot create /data
    # kopia repository connect server --url https://192.168.2.12:51515 --override-username=default --override-hostname=default  --server-cert-fingerprint B67A3489638D39810FE72FC9A28BDE064E21B93E1B982FE383E0029F80435FCE
    # TODO Configure
    command = [
        "bash", "-c", """
        kopia repository connect server \
               --url https://192.168.2.12:51515 \
               --override-username=default --override-hostname=default \
               --server-cert-fingerprint B67A3489638D39810FE72FC9A28BDE064E21B93E1B982FE383E0029F80435FCE \
                --disable-file-logging \
                --no-check-for-updates \
                --cache-directory /cache \
        && kopia snapshot create /data --override-source=/k8s/__NAMESPACE__/__APPLICATION__/
        """.replace("__APPLICATION__", application).replace("__NAMESPACE__", namespace)
    ]
    # TODO --send-snapshot-report integrate with something like healthcheck.io

    volume_mounts = [
        client.V1VolumeMount(
            name="scratch",
            mount_path="/data/scratch",
            read_only=True
        ),
        client.V1VolumeMount(
            name="cache",
            mount_path="/cache"
        ),
        client.V1VolumeMount(
            name=app_expose,
            mount_path="/data/app",
            read_only=True
        ),
        client.V1VolumeMount(
            name=data_expose,
            mount_path="/data/data",
            read_only=True
        ),
    ]

    volumes = [
        client.V1Volume(name="scratch",
                        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=scratch_volume, read_only=True)),
        client.V1Volume(name="cache",
                        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name="borg-cache")),
        #client.V1Volume(name="repository",
        #                config_map=client.V1ConfigMapVolumeSource (name="kopia-repository", default_mode=0o0400)),
        client.V1Volume(name=app_expose,
                        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=app_expose, read_only=True)),
        client.V1Volume(name=data_expose,
                        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=data_expose, read_only=True)),
    ]

    security_context = client.V1PodSecurityContext(
        run_as_group=0,
        run_as_user=0,
        run_as_non_root=False,
        seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault")
    )

    job = jobs.create_job_object(f'backup-kopia', image, command, volume_mounts, volumes,
                                 security_context=security_context, env=[
            client.V1EnvVar(name="KOPIA_PASSWORD", value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(key="password", name="kopia-repository"))),
           # client.V1EnvVar(name="KOPIA_CONFIG_PATH", value="/config/repository.config"),
        ])
    jobs.run_job(job, namespace)

    backup.delete_owned_pvcs(namespace)
    backup.delete_owned_snapshots(namespace)
    jobs.delete_owned_jobs(namespace)
