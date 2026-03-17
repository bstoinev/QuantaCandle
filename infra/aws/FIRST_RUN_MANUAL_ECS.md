# QuantaCandle First Manual ECS Run

This runbook is for manually triggering a fresh deployment of the existing trade-collector ECS service.

## 1) Apply Terraform

```bash
cd infra/aws/terraform
terraform init
terraform apply
```

## 2) Trigger deployment with the operator script

From the repository root:

```powershell
.\infra\aws\trigger-manual-trade-collection.ps1
```

The script reads Terraform outputs from `infra/aws/terraform` and triggers `aws ecs update-service --force-new-deployment`.
Use `-TerraformDir` if your Terraform state is in a different folder.

## 3) Trigger deployment with explicit cluster/service names

Example override for cluster/service targeting:

```powershell
.\infra\aws\trigger-manual-trade-collection.ps1 `
  -EcsClusterName quanta-candle `
  -EcsServiceName quanta-candle
```

## 4) What the script prints

- AWS region
- ECS cluster
- ECS service name
- ECS service ARN
- CloudWatch log group
- Collector output directory
- EFS filesystem ID
- Follow-up commands for `aws ecs describe-services` and `aws logs describe-log-streams`
