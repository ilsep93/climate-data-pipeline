# Execution steps

1. `gcloud auth application-default login`
* Ensure that correct service account is set as environment variable
  * GOOGLE_APPLICATION_CREDENTIALS is an environment variable which is set in your current shell session. If you start another terminal session (NOT via this one) that environment variable will not be set. 
* Refresh service-account's auth-token for this session
1. `terraform init`: 
    * Initializes & configures the backend, installs plugins/providers, & checks out an existing configuration from a version control 
    * Note that if GBS was specified as the backend in `main.tf`, a bucket to allocate to terraform resources must already exist.
2. `terraform plan`:
    * Matches/previews local changes against a remote state, and proposes an Execution Plan.
3. `terraform apply`: 
    * Asks for approval to the proposed plan, and applies changes to cloud
4. `terraform destroy`
    * Removes your stack from the Cloud