import os

from kubernetes import client, config

from backup import Backup, SnapshotInfo, BackupContext
from postgres import PostgresBackup


class BackupDefinition:
    def prepare_snapshots(self, ctx: BackupContext) -> dict[str, SnapshotInfo]:
        pass

def get_current_namespace() -> str:
    ns_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    if os.path.exists(ns_path):
        with open(ns_path) as f:
            return f.read().strip()

    return "default"

def create_backup(definition: BackupDefinition):
    # Configs can be set in Configuration class directly or using helper utility
    configuration = config.load_incluster_config()

    # Enter a context with an instance of the API kubernetes.client
    with client.ApiClient(configuration) as api_client:
        namespace = get_current_namespace()
        scratch_volume = "backup-scratch"  # TODO Ensure scratch is cleaned?
        application = "nextcloud"

        backup = Backup(api_client, application, namespace)
        postgres = PostgresBackup(api_client, application, namespace)

        ctx = BackupContext(backup, postgres, scratch_volume)
        snapshots = definition.prepare_snapshots(ctx)
        exposes = backup.expose_snapshots(snapshots)
        backup.run_kopia(application, scratch_volume, exposes)
        backup.cleanup()
