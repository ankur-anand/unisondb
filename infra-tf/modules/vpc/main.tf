terraform {
  required_version = ">= 1.9"
  required_providers {
    digitalocean = {
      source  = "digitalocean/digitalocean"
      version = "~> 2.0"
    }
  }
}

provider "digitalocean" {
  token = var.do_token
}

resource "digitalocean_vpc" "do_vpc" {
  name   = local.vpc_name
  region = var.region

  description = "VPC for UnisonDB cluster"
}
