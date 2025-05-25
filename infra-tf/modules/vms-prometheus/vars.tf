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

variable "droplet_size" {
  description = "The instance type to use for the droplets."
  type        = string
  default     = "s-1vcpu-1gb"
}

variable "droplet_image" {
  description = "The Docker image to use for the droplets."
  type        = string
  default     = "ubuntu-24-04-x64"
}

variable "vpc_id" {
  description = "The ID of the VPC to deploy into."
  type        = string
}

variable "prometheus_version" {
  description = "The Prometheus version to install"
  type        = string
  default     = "2.53.4"
}

variable "user_name" {
  description = "Username to log in to the droplets."
  type        = string
}

locals {
  group = "prometheus.unisondb.${var.region}-${var.env}"
  vm    = local.group
  tags  = ["unisondb", "fuzzer", var.env, var.region]
}