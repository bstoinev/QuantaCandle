data "aws_caller_identity" "current" {}

locals {
  ecr_repository_name = "${var.name_prefix}"
  cluster_name        = "${var.name_prefix}-cluster"
  task_family         = "${var.name_prefix}-collector"
  container_name      = "quanta-candle-cli"

  # Explicit CLI command arguments passed to the existing image entrypoint.
  collect_trades_command = concat(
    [
      "collect-trades",
      "--trade-source", var.trade_source,
      "--instrument", var.instrument,
      "--duration", var.duration,
      "--sink", "file",
      "--outDir", var.output_dir,
    ],
    var.binance_ws_base != "" ? ["--binanceWsBase", var.binance_ws_base] : []
  )

  scheduler_task_security_group_ids = concat(
    [aws_security_group.ecs_task.id],
    var.additional_task_security_group_ids
  )
  scheduler_assign_public_ip  = var.assign_public_ip
}

resource "aws_ecr_repository" "quanta_candle" {
  name                 = local.ecr_repository_name
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_cloudwatch_log_group" "ecs_task" {
  name              = "/ecs/${local.task_family}"
  retention_in_days = 14
}

resource "aws_ecs_cluster" "quanta_candle" {
  name = local.cluster_name
}

resource "aws_security_group" "ecs_task" {
  name        = "${var.name_prefix}-ecs-task-sg"
  description = "Security group for QuantaCandle scheduled ECS tasks."
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "efs" {
  name        = "${var.name_prefix}-efs-sg"
  description = "Security group for QuantaCandle EFS."
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 2049
    to_port         = 2049
    protocol        = "tcp"
    security_groups = [aws_security_group.ecs_task.id]
  }
}

resource "aws_efs_file_system" "quanta_candle" {
  creation_token = "${var.name_prefix}-efs"
  encrypted      = true
}

resource "aws_efs_mount_target" "quanta_candle" {
  for_each = toset(var.subnet_ids)

  file_system_id  = aws_efs_file_system.quanta_candle.id
  subnet_id       = each.value
  security_groups = [aws_security_group.efs.id]
}

resource "aws_iam_role" "ecs_task_execution" {
  name = "${var.name_prefix}-ecs-task-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_managed_policy" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "ecs_task" {
  name = "${var.name_prefix}-ecs-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_ecs_task_definition" "collector" {
  family                   = local.task_family
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.task_cpu)
  memory                   = tostring(var.task_memory)
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  volume {
    name = "efs-data"

    efs_volume_configuration {
      file_system_id     = aws_efs_file_system.quanta_candle.id
      root_directory     = "/"
      transit_encryption = "ENABLED"
    }
  }

  container_definitions = jsonencode([
    {
      name      = local.container_name
      image     = "${aws_ecr_repository.quanta_candle.repository_url}:${var.image_tag}"
      essential = true
      command   = local.collect_trades_command
      mountPoints = [
        {
          sourceVolume  = "efs-data"
          containerPath = "/data"
          readOnly      = false
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ecs_task.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])
}

resource "aws_iam_role" "scheduler_invoke" {
  name = "${var.name_prefix}-scheduler-invoke-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "scheduler.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "scheduler_invoke" {
  name = "${var.name_prefix}-scheduler-invoke-policy"
  role = aws_iam_role.scheduler_invoke.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RunCollectorTask"
        Effect = "Allow"
        Action = ["ecs:RunTask"]
        Resource = [
          aws_ecs_task_definition.collector.arn,
          "arn:aws:ecs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:task-definition/${aws_ecs_task_definition.collector.family}:*"
        ]
        Condition = {
          ArnEquals = {
            "ecs:cluster" = aws_ecs_cluster.quanta_candle.arn
          }
        }
      },
      {
        Sid    = "PassOnlyTaskRoles"
        Effect = "Allow"
        Action = ["iam:PassRole"]
        Resource = [
          aws_iam_role.ecs_task_execution.arn,
          aws_iam_role.ecs_task.arn
        ]
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "ecs-tasks.amazonaws.com"
          }
        }
      }
    ]
  })
}

resource "aws_scheduler_schedule" "collector" {
  name                         = "${var.name_prefix}-collector-schedule"
  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = var.schedule_timezone

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_ecs_cluster.quanta_candle.arn
    role_arn = aws_iam_role.scheduler_invoke.arn

    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.collector.arn
      task_count          = 1
      launch_type         = "FARGATE"
      platform_version    = "LATEST"

      network_configuration {
        subnets          = var.subnet_ids
        security_groups  = local.scheduler_task_security_group_ids
        assign_public_ip = local.scheduler_assign_public_ip
      }
    }

    input = jsonencode({
      containerOverrides = [
        {
          name    = local.container_name
          command = local.collect_trades_command
        }
      ]
    })
  }
}
