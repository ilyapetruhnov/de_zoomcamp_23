from prefect.infrastructure.docker import DockerContainer

docker_block = DockerContainer(
image = "weekcrackle/prefect:zoomcamp",
image_pull_policy="ALWAYS",
auto_remove=True
)

docker_block.save("zoomcamp",overwrite=True)
#docker_container_block = DockerContainer.load("zoomcamp")