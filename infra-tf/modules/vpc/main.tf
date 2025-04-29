terraform {
  required_providers {
    digitalocean = {
      source = "digitalocean/digitalocean"
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
