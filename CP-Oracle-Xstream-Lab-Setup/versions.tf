terraform {
  # terraform_data (built in since 1.4) drives every stage over SSH, so no
  # provider downloads are needed. The target EC2 host already exists.
  required_version = ">= 1.5.0"
}
