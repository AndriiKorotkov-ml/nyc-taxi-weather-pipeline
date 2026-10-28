import {
  to = aws_iam_role.role
  id = "nyc-taxi-weather-ec-2"
}

import {
  to = aws_iam_role_policy.policy
  id = "nyc-taxi-weather-ec-2:nyc-taxi-ec2"
}

import {
  to = aws_iam_instance_profile.profile
  id = "nyc-taxi-weather-ec-2"
}

resource "aws_iam_role" "role" {
  name        = "nyc-taxi-weather-ec-2"
  description = "Allows EC2 instances to call AWS services on your behalf."
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}


resource "aws_iam_role_policy" "policy" {
  name = "nyc-taxi-ec2"
  role = aws_iam_role.role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListIngestionPrefixes",
        Effect   = "Allow",
        Action   = "s3:ListBucket",
        Resource = aws_s3_bucket.lake.arn
        Condition = {
          StringLike = {
            "s3:prefix" = [
              "bronze/taxi/yellow/*",
              "bronze/weather/location=nyc/*"
            ]
          }
        }
      },
      {
        Sid    = "ReadWriteIngestionObjects",
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:AbortMultipartUpload"
        ],
        Resource = ["${aws_s3_bucket.lake.arn}/bronze/taxi/yellow/*",
          "${aws_s3_bucket.lake.arn}/bronze/weather/location=nyc/*"
        ]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "profile" {
  name = "nyc-taxi-weather-ec-2"
  role = aws_iam_role.role.name
}