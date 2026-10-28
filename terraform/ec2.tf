
resource "aws_instance" "airflow" {
  ami                    = "ami-042dc8681de073ac4"
  instance_type          = "m7i-flex.large"
  key_name               = "nyc-taxi-airflow-key"
  subnet_id              = data.aws_subnet.default_1a.id
  vpc_security_group_ids = [aws_security_group.airflow.id]
  iam_instance_profile   = aws_iam_instance_profile.profile.name

  tags = {
    Name = "nyc-taxi-weather-airflow"
  }
  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }
  root_block_device {
    volume_size = 30
    volume_type = "gp3"
    encrypted   = true
  }
  lifecycle {
    prevent_destroy = true
  }

}