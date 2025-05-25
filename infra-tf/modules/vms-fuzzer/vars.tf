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

variable "go_version" {
  description = "Go version to install"
  type        = string
  default     = "1.24.2"
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

variable "ops_per_namespace" {
  default = 1000
  type    = number
}

variable "workers_per_namespace" {
  default = 50
  type    = number
}

variable "local_relayer_count" {
  type = number
}

variable "git_branch" {
  description = "The git branch to use"
  type        = string
}

variable "prom_ip" {
  description = "Private IP of central prometheus server"
  type        = string
}

variable "user_name" {
  description = "Username to log in to the droplets."
  type        = string
}

variable "fuzzing_start_delay" {
  description = "Duration  to wait before fuzzing the Unisondb."
  type        = string
}

locals {
  group = "fuzzer.unisondb.${var.region}-${var.env}"
  vm    = local.group
  tags  = ["unisondb", "fuzzer", var.env, var.region]
}