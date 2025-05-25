output "vpc_name" {
  description = "The name of the attached VPC."
  value       = data.digitalocean_vpc.do_vpc.name
}

output "droplet_ips" {
  value = digitalocean_droplet.do_droplets.ipv4_address
}

output "droplet_private_ip" {
  value = digitalocean_droplet.do_droplets.ipv4_address_private
}
