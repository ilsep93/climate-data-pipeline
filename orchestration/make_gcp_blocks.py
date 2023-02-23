from prefect.filesystems import GCS
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

gcp_credentials_block = GcpCredentials.load("gcp-climate-pipeline")
gcp_zs_bucket = GCS.load("gcp-zs-bucket")