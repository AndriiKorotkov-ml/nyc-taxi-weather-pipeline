terraform {
  backend "s3" {
    bucket = "andrii-nyc-taxi-weather-tfstate"
    key          = "nyc-taxi-weather/terraform.tfstate"
    region = "eu-central-1"
    encrypt = true
    use_lockfile = true    
  }
}