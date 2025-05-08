terraform {
  required_version = ">= 1.9"
  required_providers {
    local = {
      source  = "hashicorp/local"
      version = ">= 2.5"
    }
  }
}

variable "do_token" {
  description = "The DigitalOcean API token."
  type        = string
  sensitive   = true
}

variable "ts_auth_key" {
  description = "Tail Scale AUTH Key"
  type        = string
  sensitive   = true
  default     = ""
}

variable "region" {
  description = "The region in which to deploy the infrastructure"
  type        = string
  default     = "blr1"
}

variable "env" {
  description = "The DigitalOcean environment name."
  type        = string
  default     = "dev-fuzz"
}

module "vpc" {
  source   = "./modules/vpc"
  region   = var.region
  env      = var.env
  do_token = var.do_token
}

output "vpc-out" {
  value = module.vpc
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

variable "fuzzer_droplet_size" {
  type        = string
  description = "The size slug of a droplet size"
  default     = "s-1vcpu-1gb"
}

variable "local_relayer_count" {
  type = number
}

module "fuzzer" {
  source              = "./modules/vms-fuzzer"
  do_token            = var.do_token
  ts_auth_key         = var.ts_auth_key
  vpc_id              = module.vpc.vpc_id
  ob_token            = var.ob_token
  ob_pass             = var.ob_pass
  ob_user             = var.ob_user
  droplet_size        = var.fuzzer_droplet_size
  local_relayer_count = var.local_relayer_count
}

output "fuzzer" {
  value = module.fuzzer
}

variable "client_count" {
  description = "unisondb client count"
  type        = number
  default     = 0
}

variable "client_droplet_size" {
  type        = string
  description = "The size slug of a droplet size"
  default     = "s-1vcpu-1gb"
}

module "client" {
  source       = "./modules/vms-client"
  do_token     = var.do_token
  ts_auth_key  = var.ts_auth_key
  vpc_id       = module.vpc.vpc_id
  central_ip   = module.fuzzer.droplet_private_ip
  client_count = var.client_count
  ob_token     = var.ob_token
  ob_pass      = var.ob_pass
  ob_user      = var.ob_user
  droplet_size = var.client_droplet_size
}

output "client" {
  value = module.client
}

locals {
  scrape_targets_json = jsonencode([
    for ip in concat([module.fuzzer.droplet_private_ip], values(module.client.droplet_private_ips)) : {
      targets = ["${ip}:4000"]
      labels  = { role = "unisondb" }
    }
  ])
}


resource "local_file" "scrape_targets" {
  content  = local.scrape_targets_json
  filename = "${path.module}/scrape_targets.json"
}

