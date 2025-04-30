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

module "fuzzer" {
  source      = "./modules/vms-fuzzer"
  do_token    = var.do_token
  ts_auth_key = var.ts_auth_key
  vpc_id      = module.vpc.vpc_id
}

output "fuzzer" {
  value = module.fuzzer
}

module "client" {
  source       = "./modules/vms-client"
  do_token     = var.do_token
  ts_auth_key  = var.ts_auth_key
  vpc_id       = module.vpc.vpc_id
  central_ip   = module.fuzzer.droplet_private_ip
  client_count = 2
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

