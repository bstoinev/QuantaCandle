param(
    [string]$TerraformDir = (Join-Path $PSScriptRoot "terraform"),
    [string]$AwsProfile = "quanta-candle",
    [string]$EcsClusterName = "quanta-candle",
    [string]$EcsServiceName = "quanta-candle"
)

$PSNativeCommandUseErrorActionPreference = $false

$env:AWS_PROFILE = $AwsProfile
$ErrorActionPreference = "Stop"

function Get-TerraformOutputRaw {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $value = terraform "-chdir=$($TerraformDir)" output -raw $Name 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($value)) {
        throw "Failed to read Terraform output '$Name' from '$TerraformDir'. Ensure Terraform has been applied and outputs are available."
    }

    return $value.Trim()
}

Write-Host "Reading Terraform outputs from: $TerraformDir" -ForegroundColor Cyan

$awsRegion = Get-TerraformOutputRaw -Name "aws_region"
$cloudWatchLogGroupName = Get-TerraformOutputRaw -Name "cloudwatch_log_group_name"
$collectorOutputDirectory = Get-TerraformOutputRaw -Name "collector_output_directory"
$efsFileSystemId = Get-TerraformOutputRaw -Name "efs_file_system_id"

Write-Host ""
Write-Host "Forcing ECS service deployment..." -ForegroundColor Cyan
Write-Host "Region: $awsRegion"
Write-Host "Cluster: $EcsClusterName"
Write-Host "Service: $EcsServiceName"
Write-Host "CloudWatch log group: $cloudWatchLogGroupName"
Write-Host "Output directory: $collectorOutputDirectory"
Write-Host "EFS filesystem ID: $efsFileSystemId"

$updateServiceArgs = @(
    "ecs", "update-service",
    "--region", $awsRegion,
    "--cluster", $EcsClusterName,
    "--service", $EcsServiceName,
    "--force-new-deployment"
)

$updateServiceOutput = & aws @updateServiceArgs 2>&1 | Out-String
$awsExitCode = $LASTEXITCODE
if ($awsExitCode -ne 0) {
    $awsErrorText = ($updateServiceOutput | Out-String).Trim()
    throw "aws ecs update-service failed. $awsErrorText"
}

$updateServiceResultJson = ($updateServiceOutput | Out-String).Trim()
if ([string]::IsNullOrWhiteSpace($updateServiceResultJson)) {
    throw "aws ecs update-service returned an empty response."
}

$updateServiceResult = $updateServiceResultJson | ConvertFrom-Json

if ($null -eq $updateServiceResult.service) {
    throw "aws ecs update-service response did not include a 'service' object."
}

$serviceArn = $updateServiceResult.service.serviceArn
if ([string]::IsNullOrWhiteSpace($serviceArn)) {
    throw "aws ecs update-service response did not include a service ARN."
}

Write-Host ""
Write-Host "Service deployment triggered successfully." -ForegroundColor Green
Write-Host "Service ARN: $serviceArn"
Write-Host ""
Write-Host "Follow-up commands:" -ForegroundColor Cyan
Write-Host "aws ecs describe-services --region $awsRegion --cluster $EcsClusterName --services $EcsServiceName"
Write-Host "aws logs describe-log-streams --region $awsRegion --log-group-name $cloudWatchLogGroupName --order-by LastEventTime --descending"
