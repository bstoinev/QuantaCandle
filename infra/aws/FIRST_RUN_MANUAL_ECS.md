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
  -EcsServiceName quanta-candle-service-kzkwo7xr
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

## 5) Post-trigger validation

1. Check ECS service events:

```powershell
$region = terraform -chdir=infra/aws/terraform output -raw aws_region
aws ecs describe-services `
  --region $region `
  --cluster quanta-candle `
  --services quanta-candle-service-kzkwo7xr `
  --query "services[0].events[0:10].[createdAt,message]" `
  --output table
```

2. Check the latest CloudWatch log stream in `/ecs/quanta-candle`:

```powershell
$region = terraform -chdir=infra/aws/terraform output -raw aws_region
aws logs describe-log-streams `
  --region $region `
  --log-group-name /ecs/quanta-candle `
  --order-by LastEventTime `
  --descending `
  --max-items 1
```

3. Verify output files are appearing on EFS at the expected mounted path:

```powershell
terraform -chdir=infra/aws/terraform output -raw collector_output_directory
```

Use the returned directory (for example `/data/trades-out`) and verify that new trade files are created under the mounted EFS path for that directory after deployment.
