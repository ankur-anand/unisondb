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

variable "ts_auth_key" {
  description = "Tail Scale AUTH Key"
  type        = string
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

variable "client_count" {
  description = "Number of UnisonDB client VMs to create"
  type        = number
  default     = 1
}

variable "central_ip" {
  description = "Private IP of central UnisonDB server"
  type        = string
}

variable "ob_token" {
  description = "OpenObserve token"
  type        = string
  sensitive   = true
}

variable "ob_user" {
  description = "Openobserve user"
  type        = string
  sensitive   = true
}

variable "ob_pass" {
  description = "OpenVPN user password"
  type        = string
  sensitive   = true
}

variable "prometheus_version" {
  description = "The Prometheus version to install"
  type        = string
  default     = "2.53.4"
}

locals {
  group         = "unisondb.${var.region}-${var.env}"
  firewall_name = "firewall-client-${local.group}"
  clients = {
    for i in range(1, var.client_count + 1) :
    format("client-%02d.${local.group}", i) => i
  }
  tags = ["unisondb", "client", var.env, var.region]
}