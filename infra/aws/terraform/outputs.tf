output "ecr_repository_url" {
  description = "ECR repository URL for QuantaCandle images."
  value       = aws_ecr_repository.quanta_candle.repository_url
}

output "ecs_cluster_arn" {
  description = "ECS cluster ARN used by the scheduled task."
  value       = aws_ecs_cluster.quanta_candle.arn
}

output "ecs_task_definition_arn" {
  description = "ECS task definition ARN for QuantaCandle collector runs."
  value       = aws_ecs_task_definition.collector.arn
}

output "scheduler_name" {
  description = "EventBridge Scheduler name."
  value       = aws_scheduler_schedule.collector.name
}

output "efs_file_system_id" {
  description = "EFS file system ID where collector output is persisted."
  value       = aws_efs_file_system.quanta_candle.id
}
