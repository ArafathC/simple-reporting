terraform {
  required_version = ">= 1.5"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 5.0" }
    archive = { source = "hashicorp/archive", version = "~> 2.4" }
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "name" {
  type    = string
  default = "simple-reporting"
}

variable "ingest_api_key" {
  type      = string
  default   = ""
  sensitive = true
  description = "If set, POST /events requires this in the x-api-key header."
}

data "aws_caller_identity" "me" {}

locals {
  prefix = var.name
}

# ---------- storage ----------

resource "aws_s3_bucket" "raw" {
  bucket        = "${local.prefix}-raw-${data.aws_caller_identity.me.account_id}"
  force_destroy = false
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_dynamodb_table" "speed" {
  name         = "${local.prefix}-speed"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"
  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}

resource "aws_dynamodb_table" "batch" {
  name         = "${local.prefix}-batch"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"
  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }
}

# ---------- stream + batch-layer landing (Firehose -> S3 master dataset) ----------

resource "aws_kinesis_stream" "events" {
  name             = "${local.prefix}-events"
  retention_period = 48
  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }
}

data "aws_iam_policy_document" "firehose_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${local.prefix}-firehose"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume.json
}

resource "aws_iam_role_policy" "firehose" {
  role = aws_iam_role.firehose.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:AbortMultipartUpload", "s3:GetBucketLocation", "s3:GetObject", "s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:PutObject"],
      Resource = [aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*"] },
      { Effect = "Allow", Action = ["kinesis:DescribeStream", "kinesis:GetShardIterator", "kinesis:GetRecords", "kinesis:ListShards"],
      Resource = aws_kinesis_stream.events.arn },
    ]
  })
}

resource "aws_kinesis_firehose_delivery_stream" "raw" {
  name        = "${local.prefix}-raw"
  destination = "extended_s3"
  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.events.arn
    role_arn           = aws_iam_role.firehose.arn
  }
  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = aws_s3_bucket.raw.arn
    prefix              = "raw/dt=!{timestamp:yyyy-MM-dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "errors/!{firehose:error-output-type}/dt=!{timestamp:yyyy-MM-dd}/"
    buffering_size      = 5
    buffering_interval  = 300
  }
  depends_on = [aws_iam_role_policy.firehose]
}

# ---------- lambdas ----------

data "archive_file" "src" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/build/src.zip"
  excludes    = ["__pycache__", "**/__pycache__"]
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  for_each           = toset(["ingest", "speed", "batch", "serving"])
  name               = "${local.prefix}-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "logs" {
  for_each   = aws_iam_role.lambda
  role       = each.value.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "ingest" {
  role = aws_iam_role.lambda["ingest"].id
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = ["kinesis:PutRecords", "kinesis:PutRecord"], Resource = aws_kinesis_stream.events.arn }] })
}

resource "aws_iam_role_policy" "speed" {
  role = aws_iam_role.lambda["speed"].id
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = ["dynamodb:PutItem", "dynamodb:UpdateItem"], Resource = aws_dynamodb_table.speed.arn },
    { Effect = "Allow", Action = ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:ListShards", "kinesis:ListStreams"], Resource = aws_kinesis_stream.events.arn }] })
}

resource "aws_iam_role_policy" "batch" {
  role = aws_iam_role.lambda["batch"].id
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = ["s3:GetObject"], Resource = "${aws_s3_bucket.raw.arn}/raw/*" },
    { Effect = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.raw.arn },
    { Effect = "Allow", Action = ["dynamodb:GetItem", "dynamodb:PutItem"], Resource = aws_dynamodb_table.batch.arn }] })
}

resource "aws_iam_role_policy" "serving" {
  role = aws_iam_role.lambda["serving"].id
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = ["dynamodb:GetItem", "dynamodb:Query"], Resource = [aws_dynamodb_table.batch.arn, aws_dynamodb_table.speed.arn] }] })
}

locals {
  functions = {
    ingest = {
      handler = "ingest.handler.handler"
      timeout = 10
      env     = { STREAM_NAME = aws_kinesis_stream.events.name, INGEST_API_KEY = var.ingest_api_key }
    }
    speed = {
      handler = "speed.handler.handler"
      timeout = 60
      env     = { SPEED_TABLE = aws_dynamodb_table.speed.name }
    }
    batch = {
      handler = "batch.handler.handler"
      timeout = 900
      env     = { BATCH_TABLE = aws_dynamodb_table.batch.name, RAW_BUCKET = aws_s3_bucket.raw.bucket }
    }
    serving = {
      handler = "serving.handler.handler"
      timeout = 15
      env     = { BATCH_TABLE = aws_dynamodb_table.batch.name, SPEED_TABLE = aws_dynamodb_table.speed.name }
    }
  }
}

resource "aws_lambda_function" "fn" {
  for_each         = local.functions
  function_name    = "${local.prefix}-${each.key}"
  role             = aws_iam_role.lambda[each.key].arn
  runtime          = "python3.12"
  handler          = each.value.handler
  timeout          = each.value.timeout
  memory_size      = 256
  filename         = data.archive_file.src.output_path
  source_code_hash = data.archive_file.src.output_base64sha256
  environment {
    variables = { for k, v in each.value.env : k => v if v != "" }
  }
}

# speed layer: Kinesis -> Lambda
resource "aws_lambda_event_source_mapping" "speed" {
  event_source_arn                   = aws_kinesis_stream.events.arn
  function_name                      = aws_lambda_function.fn["speed"].arn
  starting_position                  = "LATEST"
  batch_size                         = 100
  maximum_batching_window_in_seconds = 2
  function_response_types            = ["ReportBatchItemFailures"]
  maximum_retry_attempts             = 5
  bisect_batch_on_function_error     = true
}

# batch layer: hourly schedule
resource "aws_cloudwatch_event_rule" "hourly" {
  name                = "${local.prefix}-batch-hourly"
  schedule_expression = "cron(10 * * * ? *)"
}

resource "aws_cloudwatch_event_target" "batch" {
  rule = aws_cloudwatch_event_rule.hourly.name
  arn  = aws_lambda_function.fn["batch"].arn
}

resource "aws_lambda_permission" "events_batch" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.fn["batch"].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.hourly.arn
}

# ---------- HTTP API ----------

resource "aws_apigatewayv2_api" "http" {
  name          = local.prefix
  protocol_type = "HTTP"
  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["GET", "POST"]
    allow_headers = ["content-type", "x-api-key"]
  }
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.http.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_apigatewayv2_integration" "fn" {
  for_each               = toset(["ingest", "serving"])
  api_id                 = aws_apigatewayv2_api.http.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.fn[each.key].invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "routes" {
  for_each = {
    "POST /events"  = "ingest"
    "GET /metrics"  = "serving"
    "GET /dashboard" = "serving"
  }
  api_id    = aws_apigatewayv2_api.http.id
  route_key = each.key
  target    = "integrations/${aws_apigatewayv2_integration.fn[each.value].id}"
}

resource "aws_lambda_permission" "apigw" {
  for_each      = toset(["ingest", "serving"])
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.fn[each.key].function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.http.execution_arn}/*/*"
}

output "api_url" {
  value = aws_apigatewayv2_api.http.api_endpoint
}

output "dashboard_url" {
  value = "${aws_apigatewayv2_api.http.api_endpoint}/dashboard"
}
