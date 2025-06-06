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
  name              = local.vm
  image             = var.droplet_image
  size              = var.droplet_size
  region            = var.region
  monitoring        = true # comes free of cost.
  ipv6              = false
  graceful_shutdown = true
  ssh_keys          = [data.digitalocean_ssh_key.do_ssh_key.fingerprint]
  vpc_uuid          = var.vpc_id
  tags              = local.tags

  user_data = templatefile("${path.module}/cloud-init.yml", {
    username              = var.user_name,
    ssh_public_key        = data.digitalocean_ssh_key.do_ssh_key.public_key,
    id                    = local.vm
    region                = var.region
    env                   = var.env
    go_version            = var.go_version
    prometheus_version    = var.prometheus_version
    role                  = "fuzzer"
    local_relayer_count   = var.local_relayer_count
    workers_per_namespace = var.workers_per_namespace
    ops_per_namespace     = var.ops_per_namespace
    central_prometheus_ip = var.prom_ip
    branch                = var.git_branch
    fuzzing_start_delay   = var.fuzzing_start_delay
  })

}
