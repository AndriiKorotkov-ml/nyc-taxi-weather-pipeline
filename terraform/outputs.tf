output "s3_bucket_name" {
  description = "Name of a bucket"
  value       = aws_s3_bucket.lake.id
}
output "s3_bucket_arn" {
  description = "ARN of bucket"
  value       = aws_s3_bucket.lake.arn
}
output "ec2_instance_id" {
  description = "Id of EC2 instance"
  value       = aws_instance.airflow.id
}
output "ec2_public_ip" {
  description = "Public IP of EC2"
  value       = aws_instance.airflow.public_ip
}

output "iam_role_arn" {
  description = "ARN of a role"
  value       = aws_iam_role.role.arn
}