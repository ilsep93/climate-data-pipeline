from prefect.filesystems import GitHub
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

github_block = GitHub.load("climate-pipeline-repo")
gcp_credentials_block = GcpCredentials.load("gcp-climate-pipeline")
gcp_zs_bucket = GcsBucket.load("gcp-zs-bucket")