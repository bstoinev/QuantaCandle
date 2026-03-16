# QuantaCandle First Manual ECS Run

This runbook is for the first real manual trade-collection task execution with the existing CLI-based ECS task definition.

## 1) Apply Terraform

```bash
cd infra/aws/terraform
terraform init
terraform apply
```

## 2) Inspect key outputs

```bash
terraform output aws_region
terraform output ecs_cluster_name
terraform output ecs_task_definition_arn
terraform output ecs_container_name
terraform output ecs_private_subnet_ids
terraform output ecs_task_security_group_id
terraform output cloudwatch_log_group_name
terraform output efs_file_system_id
terraform output collector_output_directory
```

## 3) Run one task manually (exact command)

```bash
aws ecs run-task \
  --region "$(terraform output -raw aws_region)" \
  --cluster "$(terraform output -raw ecs_cluster_name)" \
  --launch-type FARGATE \
  --task-definition "$(terraform output -raw ecs_task_definition_arn)" \
  --network-configuration "awsvpcConfiguration={subnets=[$(terraform output -raw ecs_private_subnet_ids_csv)],securityGroups=[$(terraform output -raw ecs_task_security_group_id)],assignPublicIp=$(terraform output -raw ecs_assign_public_ip_mode)}"
```

## 4) Verify logs

- CloudWatch Logs group: `$(terraform output -raw cloudwatch_log_group_name)`
- In ECS task logs, expect normal startup and trade collection output from the CLI command.

## 5) Verify persisted files

- EFS filesystem ID: `$(terraform output -raw efs_file_system_id)`
- Expected in-container/EFS output path: `$(terraform output -raw collector_output_directory)`
- After task completion, expect instrument/day JSONL files under that path (for example `.../BTC-USDT/YYYY-MM-DD.jsonl`).

## 6) What success looks like

- `aws ecs run-task` returns a started task (no placement/permission errors).
- Task transitions to `STOPPED` with container exit code `0`.
- CloudWatch logs show trade collection summary lines.
- Trade files are present on EFS at the configured output path.
