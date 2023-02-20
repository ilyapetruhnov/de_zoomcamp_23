from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from gcs_to_bq import etl_parent_flow

docker_container_block = DockerContainer.load("zoomcamp")

docker_deployment = Deployment.build_from_flow(
    flow = etl_parent_flow, 
    name="docker-etl_gcs_bq", 
    infrastructure = docker_container_block
)

if __name__ == "__main__":
    docker_deployment.apply()