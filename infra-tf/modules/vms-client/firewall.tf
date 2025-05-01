# Create a firewall to allow SSH (port 22) and Salt (port 4505-4506) traffic
resource "digitalocean_firewall" "do_firewall" {
  name = local.firewall_name

  droplet_ids = [
    for droplet in digitalocean_droplet.do_droplets :
    droplet.id
  ]

  inbound_rule {
    protocol   = "tcp"
    port_range = "22"
    # Restrict to Tailscale network
    # source_addresses = ["100.64.0.0/10", "::/0"]
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  inbound_rule {
    protocol   = "tcp"
    port_range = "1-65535"
    # Allow all traffic from the VPC
    source_addresses = [data.digitalocean_vpc.do_vpc.ip_range]
  }

  inbound_rule {
    protocol   = "udp"
    port_range = "53"
    # Allow all traffic from the VPC
    source_addresses = [data.digitalocean_vpc.do_vpc.ip_range]
  }

  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "udp"
    port_range            = "53"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  tags = ["unisondb", var.env, var.region]
}