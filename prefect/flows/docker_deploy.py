from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from parametrized_etl import etl_parent_flow

docker_container_block = DockerContainer.load("zoomcamp")

docker_deployment = Deployment.build_from_flow(
    flow = etl_parent_flow, 
    name="docker-flow", 
    infrastructure = docker_container_block
)

if __name__ == "__main__":
    docker_deployment.apply()