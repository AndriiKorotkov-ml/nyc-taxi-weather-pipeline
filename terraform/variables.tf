variable "region" {
  description = "Region of aws"
  type        = string
  default     = "eu-central-1"
}
variable "project" {
  description = "project name"
  type        = string
  default = "nyc-taxi-weather"
}
variable "my_ip" {
  description = "Public  IP for SSH access"
  type        = string
}