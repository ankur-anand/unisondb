output "vpc_name" {
  description = "The name of the created VPC."
  value       = digitalocean_vpc.do_vpc.name
}

output "vpc_cidr" {
  description = "The CIDR block of the created VPC."
  value       = digitalocean_vpc.do_vpc.ip_range
}

output "vpc_id" {
  description = "The ID of the created VPC."
  value       = digitalocean_vpc.do_vpc.id
}