variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
}

variable "name_prefix" {
  description = "Prefix used for QuantaCandle AWS resources."
  type        = string
  default     = "quanta-candle"
}

variable "image_tag" {
  description = "ECR image tag used by the ECS task definition."
  type        = string
  default     = "latest"
}

variable "vpc_id" {
  description = "VPC ID used for ECS task and EFS security groups."
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs used both for ECS task networking and EFS mount targets."
  type        = list(string)
}

variable "additional_task_security_group_ids" {
  description = "Optional additional security groups to attach to the Fargate task ENI."
  type        = list(string)
  default     = []
}

variable "assign_public_ip" {
  description = "Whether the scheduled Fargate task should get a public IP."
  type        = bool
  default     = false
}

variable "schedule_expression" {
  description = "EventBridge Scheduler expression, for example rate(5 minutes) or cron(0/5 * * * ? *)."
  type        = string
  default     = "rate(5 minutes)"
}

variable "schedule_timezone" {
  description = "Timezone for the EventBridge Scheduler expression."
  type        = string
  default     = "UTC"
}

variable "source" {
  description = "CLI --source argument for collect-trades."
  type        = string
  default     = "binance"
}

variable "instrument" {
  description = "CLI --instrument argument for collect-trades."
  type        = string
  default     = "BTCUSDT"
}

variable "duration" {
  description = "CLI --duration argument for collect-trades."
  type        = string
  default     = "30s"
}

variable "output_dir" {
  description = "CLI --outDir argument for collect-trades. This should point to the mounted EFS path."
  type        = string
  default     = "/data/trades-out"
}

variable "binance_ws_base" {
  description = "Optional CLI --binanceWsBase argument. Leave empty to use the app default."
  type        = string
  default     = ""
}

variable "task_cpu" {
  description = "Fargate task CPU units."
  type        = number
  default     = 512
}

variable "task_memory" {
  description = "Fargate task memory in MiB."
  type        = number
  default     = 1024
}
