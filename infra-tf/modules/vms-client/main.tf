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
  ipv6              = false
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
    go_version         = var.go_version
    central_ip         = var.central_ip
    prometheus_version = var.prometheus_version
    role               = "client"
    branch             = var.git_branch
  })

  connection {
    type        = "ssh"
    user        = "ankur"
    private_key = file(var.ssh_private_key_path)
    host        = self.ipv4_address
  }

  provisioner "file" {
    source      = "${path.module}/generate_configs.sh"
    destination = "/tmp/setup_unisondb.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "echo 'Waiting for cloud-init to complete...'",
      "while [ ! -f /var/lib/cloud/instance/boot-finished ]; do sleep 5; done",
      "echo 'cloud-init completed.'",
      "chmod +x /tmp/setup_unisondb.sh",
      "sudo CENTRAL_IP=${var.central_ip} INSTANCE_COUNT=${var.instance_count} USERNAME=${var.user_name} PROM_IP=${var.prom_ip} ROLE=client /tmp/setup_unisondb.sh"
    ]
  }

  lifecycle {
    ignore_changes = [user_data]
  }
}
