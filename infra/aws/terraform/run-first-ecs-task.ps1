param(
    [string]$TerraformDir = ".",
    [string]$Count = "1",
    [string]$StartedBy = "manual-first-run",
    [string]$CommandOverrideJson = ""
)

$ErrorActionPreference = "Stop"

function Get-TerraformOutputRaw {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $value = terraform -chdir=$TerraformDir output -raw $Name

    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($value)) {
        throw "Failed to read Terraform output '$Name'."
    }

    return $value.Trim()
}

function Quote-JsonString {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Value
    )

    return '"' + ($Value -replace '\\', '\\' -replace '"', '\\"') + '"'
}

Write-Host "Reading Terraform outputs from: $TerraformDir" -ForegroundColor Cyan

$awsRegion = Get-TerraformOutputRaw -Name "aws_region"
$ecsClusterName = Get-TerraformOutputRaw -Name "ecs_cluster_name"
$ecsTaskDefinitionArn = Get-TerraformOutputRaw -Name "ecs_task_definition_arn"
$ecsContainerName = Get-TerraformOutputRaw -Name "ecs_container_name"
$ecsPrivateSubnetIdsCsv = Get-TerraformOutputRaw -Name "ecs_private_subnet_ids_csv"
$ecsTaskSecurityGroupId = Get-TerraformOutputRaw -Name "ecs_task_security_group_id"
$ecsAssignPublicIpMode = Get-TerraformOutputRaw -Name "ecs_assign_public_ip_mode"
$cloudWatchLogGroupName = Get-TerraformOutputRaw -Name "cloudwatch_log_group_name"
$collectorOutputDirectory = Get-TerraformOutputRaw -Name "collector_output_directory"
$efsFileSystemId = Get-TerraformOutputRaw -Name "efs_file_system_id"

$subnetJson = ($ecsPrivateSubnetIdsCsv.Split(',') | ForEach-Object { Quote-JsonString -Value $_.Trim() }) -join ","
$securityGroupJson = Quote-JsonString -Value $ecsTaskSecurityGroupId
$assignPublicIpJson = if ($ecsAssignPublicIpMode.ToUpperInvariant() -eq "ENABLED") { "ENABLED" } else { "DISABLED" }

$networkConfigurationObject = @{
    awsvpcConfiguration = @{
        subnets = @()
        securityGroups = @($ecsTaskSecurityGroupId)
        assignPublicIp = $assignPublicIpJson
    }
}

$networkConfigurationObject.awsvpcConfiguration.subnets = $ecsPrivateSubnetIdsCsv.Split(',') | ForEach-Object { $_.Trim() }
$networkConfigurationJson = $networkConfigurationObject | ConvertTo-Json -Depth 5 -Compress

$runTaskArgs = @(
    "ecs", "run-task",
    "--region", $awsRegion,
    "--cluster", $ecsClusterName,
    "--launch-type", "FARGATE",
    "--task-definition", $ecsTaskDefinitionArn,
    "--count", $Count,
    "--started-by", $StartedBy,
    "--network-configuration", $networkConfigurationJson
)

if (-not [string]::IsNullOrWhiteSpace($CommandOverrideJson)) {
    $runTaskArgs += @("--overrides", $CommandOverrideJson)
}

Write-Host ""
Write-Host "Running ECS task..." -ForegroundColor Cyan
Write-Host "Cluster: $ecsClusterName"
Write-Host "Task definition: $ecsTaskDefinitionArn"
Write-Host "Container: $ecsContainerName"
Write-Host "Log group: $cloudWatchLogGroupName"
Write-Host "Output directory: $collectorOutputDirectory"
Write-Host "EFS file system: $efsFileSystemId"
Write-Host "Network configuration: $networkConfigurationJson"

$runTaskResultJson = & aws @runTaskArgs

if ($LASTEXITCODE -ne 0) {
    throw "aws ecs run-task failed."
}

$runTaskResult = $runTaskResultJson | ConvertFrom-Json

if ($null -ne $runTaskResult.failures -and $runTaskResult.failures.Count -gt 0) {
    Write-Host ""
    Write-Host "ECS returned failures:" -ForegroundColor Red
    $runTaskResult.failures | ConvertTo-Json -Depth 10
    throw "Task was not started successfully."
}

if ($null -eq $runTaskResult.tasks -or $runTaskResult.tasks.Count -eq 0) {
    throw "No ECS tasks were returned by run-task."
}

$taskArn = $runTaskResult.tasks[0].taskArn

Write-Host ""
Write-Host "Task started." -ForegroundColor Green
Write-Host "Task ARN: $taskArn"
Write-Host ""
Write-Host "Next commands:" -ForegroundColor Cyan
Write-Host "aws ecs describe-tasks --region $awsRegion --cluster $ecsClusterName --tasks $taskArn"
Write-Host "aws logs describe-log-streams --region $awsRegion --log-group-name $cloudWatchLogGroupName --order-by LastEventTime --descending"
Write-Host ""
Write-Host "Validation targets:" -ForegroundColor Cyan
Write-Host "- CloudWatch log group: $cloudWatchLogGroupName"
Write-Host "- EFS file system: $efsFileSystemId"
Write-Host "- Collector output directory inside mounted EFS: $collectorOutputDirectory"
