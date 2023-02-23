from prefect.blocks.core import Block
from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from raster_ingestion import etl_parent_flow

github_block = GitHub.load("github-repo")

deployment = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="github_multi_green_taxi_flow",
    parameters={"color": "green", "year": 2020, "months": [1,2,3]},
    infra_overrides={"env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}},
    work_queue_name="default",
    storage=github_block
)

if __name__ == "__main__":
    deployment.apply()