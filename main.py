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

    image = "ghcr.io/flx5/k8s-borg/borgbackup:main@sha256:733869d6b3c879d5ee446a5f17ff6c98fec4e03550fd17f4a7c20d7addc2f088"

    command = [
        # TODO Check host key
        "borg", "create",
        "--rsh=ssh -oBatchMode=yes -o StrictHostKeyChecking=no -i /secret/sshkey",
        "-v", "--progress", "--stats",
        "::'{now:%Y-%m-%d_%H:%M}",
        "/data"
    ]

    volume_mounts = [
        client.V1VolumeMount(
            name="scratch",
            mount_path="/data/scratch",
            read_only=True
        ),
        client.V1VolumeMount(
            name="borg",
            mount_path="/borg"
        ),
        client.V1VolumeMount(
            name="secret",
            mount_path="/secret"
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
                        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name="borg-cache")),
        client.V1Volume(name="secret",
                        secret=client.V1SecretVolumeSource(secret_name="borg-repository1", default_mode=0o0400)),
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

    job = jobs.create_job_object(f'backup-borg', image, command, volume_mounts, volumes,
                                 security_context=security_context, env=[
            client.V1EnvVar(name="BORG_REPO", value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(key="repository", name="borg-repository1"))),
            client.V1EnvVar(name="BORG_PASSPHRASE", value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(key="passphrase", name="borg-repository1"))),
        ])
    jobs.run_job(job, namespace)

    backup.delete_owned_pvcs(namespace)
    backup.delete_owned_snapshots(namespace)
    jobs.delete_owned_jobs(namespace)
