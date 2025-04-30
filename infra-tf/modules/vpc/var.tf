variable "do_token" {
  description = "The DigitalOcean API token."
  type        = string
  sensitive   = true
}

variable "region" {
  description = "The DigitalOcean region to deploy the resources in."
  type        = string
  default     = "blr1"
}

variable "env" {
  description = "The environment for the deployment (e.g., dev, staging, prod)"
  type        = string
  default     = "dev" # Default to 'dev' if not specified
}

locals {
  group    = "do.unisondb-${var.env}-${var.region}"
  vpc_name = "vpc-${local.group}"
}
