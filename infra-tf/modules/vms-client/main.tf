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

data "digitalocean_ssh_key" "do_ssh_key" {
  name = "an-macbook-14pro"
}

resource "digitalocean_droplet" "do_droplets" {
  for_each = local.clients

  name              = each.key
  image             = var.droplet_image
  size              = var.droplet_size
  region            = var.region
  monitoring        = true # comes free of cost.
  ipv6              = true
  graceful_shutdown = true
  ssh_keys          = [data.digitalocean_ssh_key.do_ssh_key.fingerprint]
  vpc_uuid          = var.vpc_id
  tags              = local.tags

  user_data = templatefile("${path.module}/cloud-init.yml", {
    username           = "ankur",
    ssh_public_key     = data.digitalocean_ssh_key.do_ssh_key.public_key,
    id                 = each.key
    region             = var.region
    env                = var.env
    ts_auth_key        = var.ts_auth_key
    go_version         = var.go_version
    central_ip         = var.central_ip
    prometheus_version = var.prometheus_version
    ob_token           = var.ob_token
    ob_user            = var.ob_user
    ob_pass            = var.ob_pass
    role               = "client"
  })

}
