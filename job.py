from time import sleep, time
from typing import List

from kubernetes import client
from kubernetes.client import V1Job, CoreV1Api
from kubernetes.watch import watch

from backup import BACKUP_OWNER_LABEL

class BackupJob:
    def __init__(self, apiClient, owner):
        self.client = apiClient
        self.batch_v1 = client.BatchV1Api(apiClient)
        self.core_v1: CoreV1Api = client.CoreV1Api(apiClient)
        self.owner = owner

    def create_job_object(self, name: str, image: str, command: List[str],
                          mounts: List[client.V1VolumeMount],
                          volumes: List[client.V1Volume], env, security_context = None) -> client.V1Job:
        # Configure Pod template container
        container = client.V1Container(
            name="container",
            image=image,
            command=command,
            volume_mounts=mounts,
        env=env)

        if security_context is None:
            security_context = client.V1PodSecurityContext(
                fs_group=1000,
                run_as_group=1000,
                run_as_user=1000,
                fs_group_change_policy="OnRootMismatch",
                run_as_non_root=True,
                seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault")
            )

        # Create and configure a spec section
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(
                labels={
                    BACKUP_OWNER_LABEL: self.owner
                }
            ),
            spec=client.V1PodSpec(
                restart_policy="Never",
                containers=[container],
                volumes=volumes,
                security_context=security_context
            )
        )
        # Create the specification of deployment
        spec = client.V1JobSpec(
            template=template,
            backoff_limit=4)
        # Instantiate the job object
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(generate_name=name),
            spec=spec)

        return job


    def run_job(self, job, namespace):
        job: V1Job = self.batch_v1.create_namespaced_job(
            body=job,
            namespace=namespace)

        print(f'Executing job {job.metadata.name}')
        start_time = time()

        print("Waiting for Pod to initialize...")
        pod_name = None
        while True:
            pods = self.core_v1.list_namespaced_pod(namespace=namespace, label_selector=f"job-name={job.metadata.name}")
            if pods.items:
                pod = pods.items[0]
                pod_name = pod.metadata.name
                # Wait until the Pod is no longer 'Pending'
                if pod.status.phase != "Pending":
                    break
            sleep(1)

        # 4. Stream the logs to stdout
        print(f"Streaming logs from Pod '{pod_name}':\n" + "-" * 30)
        w = watch.Watch()
        try:
            for line in w.stream(self.core_v1.read_namespaced_pod_log, name=pod_name, namespace=namespace, follow=True):
                print(line)
        finally:
            w.stop()

        print("Logs ended")
        w = watch.Watch()

        # Stream events for pods in the given namespace
        # Use v1.list_pod_for_all_namespaces for cluster-wide watching
        selector = "metadata.name="+job.metadata.name

        for event in w.stream(self.batch_v1.list_namespaced_job,
                              resource_version=job.metadata.resource_version,
                              field_selector=selector,
                              namespace=namespace):
            obj = event['object']

            if obj.status.succeeded is not None or \
                    obj.status.failed is not None:
                w.stop()
                end_time = time()

                elapsed_time = end_time - start_time
                print(f"Job {job.metadata.name} finished in {elapsed_time} seconds")

                if obj.status.failed is not None:
                    raise Exception(f'Job {job.metadata.name} failed')

    def delete_owned_jobs(self, namespace):
        self.batch_v1.delete_collection_namespaced_job(
            namespace=namespace,
            label_selector=f'{BACKUP_OWNER_LABEL}={self.owner}')