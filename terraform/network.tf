data "aws_vpc" "default" {
  default = true
}
data "aws_subnet" "default_1a" {
  vpc_id            = data.aws_vpc.default.id
  availability_zone = "eu-central-1a"
  default_for_az    = true
}


resource "aws_security_group" "airflow" {
  name        = "nyc-taxi-airflow-sg"
  description = "launch-wizard-1 created 2026-10-21T09:35:27.765Z"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.my_ip]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}