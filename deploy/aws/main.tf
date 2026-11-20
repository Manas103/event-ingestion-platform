# One short-lived AWS environment for the platform: an ECR repository for the
# image, an S3 bucket for results, and one free-tier-eligible EC2 instance
# that runs Kafka, Redis, two gateways and two appliers in Docker, drives the
# devicesim fleet against them (a baseline and a gateway crash), verifies the
# final state, uploads the logs to S3, and powers off. scripts/aws_run.sh
# applies this, pushes the image, collects the results and destroys it all.
#
# Access is through SSM only: the security group allows no inbound traffic.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
  }
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "instance_type" {
  type    = string
  default = "m7i-flex.large" # free-tier eligible on this account: 2 vCPU, 8 GiB
}

variable "run_id" {
  type        = string
  description = "names the results prefix and every resource of one run"
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = "event-ingestion-platform", run = var.run_id }
  }
}

data "aws_caller_identity" "me" {}

data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

data "aws_vpc" "default" {
  default = true
}

resource "aws_ecr_repository" "eip" {
  name                 = "event-ingestion-platform"
  force_delete         = true
  image_tag_mutability = "MUTABLE"
}

resource "aws_s3_bucket" "results" {
  bucket        = "eip-results-${data.aws_caller_identity.me.account_id}-${var.run_id}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "results" {
  bucket                  = aws_s3_bucket.results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_iam_role" "node" {
  name = "eip-node-${var.run_id}"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "ec2.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "ecr" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

resource "aws_iam_role_policy" "results" {
  name = "write-results"
  role = aws_iam_role.node.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:PutObject"]
      Resource = "${aws_s3_bucket.results.arn}/*"
    }]
  })
}

resource "aws_iam_instance_profile" "node" {
  name = "eip-node-${var.run_id}"
  role = aws_iam_role.node.name
}

resource "aws_security_group" "node" {
  name        = "eip-node-${var.run_id}"
  description = "no inbound; outbound for image pulls, S3 and SSM"
  vpc_id      = data.aws_vpc.default.id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "node" {
  ami                                  = data.aws_ssm_parameter.al2023.value
  instance_type                        = var.instance_type
  iam_instance_profile                 = aws_iam_instance_profile.node.name
  vpc_security_group_ids               = [aws_security_group.node.id]
  associate_public_ip_address          = true
  instance_initiated_shutdown_behavior = "stop"
  user_data_replace_on_change          = true
  user_data = templatefile("${path.module}/run.sh.tftpl", {
    region   = var.region
    registry = split("/", aws_ecr_repository.eip.repository_url)[0]
    image    = "${aws_ecr_repository.eip.repository_url}:latest"
    bucket   = aws_s3_bucket.results.bucket
    run_id   = var.run_id
  })
  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }
  metadata_options {
    http_tokens = "required"
  }
}

output "ecr_repository_url" {
  value = aws_ecr_repository.eip.repository_url
}

output "results_bucket" {
  value = aws_s3_bucket.results.bucket
}

output "instance_id" {
  value = aws_instance.node.id
}
