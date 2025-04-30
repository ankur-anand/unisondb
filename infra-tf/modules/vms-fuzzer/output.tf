output "vpc_name" {
  description = "The name of the attached VPC."
  value       = data.digitalocean_vpc.do_vpc.name
}

output "firewall" {
  value = local.firewall_name
}

output "droplet_ips" {
  value = digitalocean_droplet.do_droplets.ipv4_address
}
