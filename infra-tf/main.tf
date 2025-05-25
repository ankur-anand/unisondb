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

variable "fuzzer_droplet_size" {
  type        = string
  description = "The size slug of a droplet size"
  default     = "s-1vcpu-1gb"
}

variable "local_relayer_count" {
  type = number
}

variable "git_branch" {
  description = "The git branch to use"
  type        = string
  default     = "main"
}

variable "user_name" {
  description = "Username to log in to the droplets."
  type        = string
  default     = "ankur"
}

module "prometheus" {
  source = "./modules/vms-prometheus"

  do_token  = var.do_token
  vpc_id    = module.vpc.vpc_id
  user_name = var.user_name
}

output "prometheus" {
  value = module.prometheus
}

variable "fuzzing_start_delay" {
  description = "Duration  to wait before fuzzing the Unisondb."
  type        = string
}

module "fuzzer" {
  source              = "./modules/vms-fuzzer"
  do_token            = var.do_token
  vpc_id              = module.vpc.vpc_id
  droplet_size        = var.fuzzer_droplet_size
  local_relayer_count = var.local_relayer_count
  prom_ip             = module.prometheus.droplet_private_ip
  git_branch          = var.git_branch
  user_name           = var.user_name
  fuzzing_start_delay = var.fuzzing_start_delay
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

variable "ssh_private_key_path" {
  description = "Path to the SSH private key used for connecting to droplets."
  type        = string
}

variable "instance_count" {
  description = "Number of instances to launch on each vm for replication"
  type        = number
}


module "client" {
  source               = "./modules/vms-client"
  do_token             = var.do_token
  vpc_id               = module.vpc.vpc_id
  central_ip           = module.fuzzer.droplet_private_ip
  client_count         = var.client_count
  droplet_size         = var.client_droplet_size
  git_branch           = var.git_branch
  ssh_private_key_path = var.ssh_private_key_path
  instance_count       = var.instance_count
  prom_ip              = module.prometheus.droplet_private_ip
  user_name            = var.user_name
}

output "client" {
  value = module.client
}
