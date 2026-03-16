output "aws_region" {
  description = "AWS region used by this Terraform stack."
  value       = var.aws_region
}

output "ecr_repository_url" {
  description = "ECR repository URL for QuantaCandle images."
  value       = aws_ecr_repository.quanta_candle.repository_url
}

output "ecs_cluster_name" {
  description = "ECS cluster name used by the scheduled task."
  value       = aws_ecs_cluster.quanta_candle.name
}

output "ecs_cluster_arn" {
  description = "ECS cluster ARN used by the scheduled task."
  value       = aws_ecs_cluster.quanta_candle.arn
}

output "ecs_task_definition_arn" {
  description = "ECS task definition ARN for QuantaCandle collector runs."
  value       = aws_ecs_task_definition.collector.arn
}

output "ecs_task_definition_family" {
  description = "ECS task definition family name."
  value       = aws_ecs_task_definition.collector.family
}

output "ecs_container_name" {
  description = "Container name used by the ECS task definition."
  value       = local.container_name
}

output "ecs_private_subnet_ids" {
  description = "Private subnet IDs used for ECS task networking."
  value       = var.subnet_ids
}

output "ecs_private_subnet_ids_csv" {
  description = "Private subnet IDs as CSV, useful for manual aws ecs run-task commands."
  value       = join(",", var.subnet_ids)
}

output "ecs_task_security_group_id" {
  description = "Primary ECS task security group ID."
  value       = aws_security_group.ecs_task.id
}

output "ecs_assign_public_ip_mode" {
  description = "AssignPublicIp mode to use for manual aws ecs run-task."
  value       = var.assign_public_ip ? "ENABLED" : "DISABLED"
}

output "cloudwatch_log_group_name" {
  description = "CloudWatch Logs group name used by the ECS task container."
  value       = aws_cloudwatch_log_group.ecs_task.name
}

output "scheduler_name" {
  description = "EventBridge Scheduler name."
  value       = aws_scheduler_schedule.collector.name
}

output "efs_file_system_id" {
  description = "EFS file system ID where collector output is persisted."
  value       = aws_efs_file_system.quanta_candle.id
}

output "collector_output_directory" {
  description = "Configured CLI output directory inside the container/EFS mount."
  value       = var.output_dir
}
