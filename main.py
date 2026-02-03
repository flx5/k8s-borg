from kubernetes import client, config

from backup import Backup
from job import BackupJob
from postgres import PostgresBackup

# Configs can be set in Configuration class directly or using helper utility
configuration = config.load_kube_config()

# Enter a context with an instance of the API kubernetes.client
with client.ApiClient(configuration) as api_client:
    owner = "testbackup"
    namespace = "nextcloud"
    scratch_volume = "nextcloud-scratch"

    backup = Backup(api_client, owner)
    postgres = PostgresBackup(api_client, owner)
    jobs = BackupJob(api_client, owner)

    #backup.exec_in_single_deployment_pod("nextcloud",
    #                                                ["./occ", "maintenance:mode", "--on"], namespace)

    try:
        data_snapshot = backup.create_snapshot("nextcloud-data", namespace)
        app_snapshot = backup.create_snapshot("nextcloud-app", namespace)
        # postgres.dump_postgres("pg-nextcloud", "nextcloud", scratch_volume=scratch_volume)

        backup.wait_for_snapshot(data_snapshot, namespace)
        backup.wait_for_snapshot(app_snapshot, namespace)
    finally:
        pass
        #backup.exec_in_single_deployment_pod("nextcloud",
        #                                     ["./occ", "maintenance:mode", "--off"], namespace)

    app_expose = backup.expose_snapshot(app_snapshot, namespace)
    data_expose = backup.expose_snapshot(data_snapshot, namespace)

    image = "ghcr.io/flx5/k8s-borg/borgbackup:sha256-cb33495ec8062f14de601bf2ff2038f2f0ae1c38c5f93f2b0246332793df7bdf.sig"

    command = [
        "id"
    ]

    volume_mounts = [
        client.V1VolumeMount(
            name="scratch",
            mount_path="/data/scratch",
            read_only=True
        ),
        client.V1VolumeMount(
            name="borg",
            mount_path="/borg",
            read_only=True
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
        client.V1Volume(name="borg",
                        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name="borg-cache",
                                                                                           read_only=True)),
        client.V1Volume(name=app_expose,
                        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=app_expose, read_only=True)),
        client.V1Volume(name=data_expose,
                        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=data_expose, read_only=True)),
    ]

    job = jobs.create_job_object(f'backup-borg', image, command, volume_mounts, volumes, env=[])
    jobs.run_job(job, namespace)

    backup.delete_owned_pvcs(namespace)
    backup.delete_owned_snapshots(namespace)
    jobs.delete_owned_jobs(namespace)
