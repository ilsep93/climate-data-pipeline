from prefect.filesystems import GCS, GitHub
from prefect_gcp import GcpCredentials

github_block = GitHub.load("climate-pipeline-repo")
gcp_credentials_block = GcpCredentials.load("gcp-climate-pipeline")
gcp_zs_bucket = GCS.load("gcp-zs-bucket")