terraform {
  required_version = ">= 1.9"
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