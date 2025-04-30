output "vpc_name" {
  description = "The name of the attached VPC."
  value       = data.digitalocean_vpc.do_vpc.name
}

output "firewall" {
  value = local.firewall_name
}

output "droplet_ips" {
  value = {
    for name, droplet in digitalocean_droplet.do_droplets :
    name => droplet.ipv4_address
  }
}

output "droplet_private_ips" {
  value = {
    for name, droplet in digitalocean_droplet.do_droplets :
    name => droplet.ipv4_address_private
  }
}
