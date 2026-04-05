resource "random_password" "admin" {
  length  = 24
  special = false
}

resource "aws_secretsmanager_secret" "admin" {
  name                    = var.admin_secret_name
  recovery_window_in_days = 7
  tags                    = merge(var.tags, { Name = var.admin_secret_name })
}

resource "aws_secretsmanager_secret_version" "admin" {
  secret_id = aws_secretsmanager_secret.admin.id
  secret_string = jsonencode({
    username = var.admin_username
    password = random_password.admin.result
    database = var.database_name
  })
}

resource "aws_redshiftserverless_namespace" "this" {
  namespace_name      = var.namespace_name
  db_name             = var.database_name
  admin_username      = var.admin_username
  admin_user_password = random_password.admin.result
  iam_roles           = [var.copy_role_arn]
  tags                = merge(var.tags, { Name = var.namespace_name })
}

resource "aws_redshiftserverless_workgroup" "this" {
  workgroup_name       = var.workgroup_name
  namespace_name       = aws_redshiftserverless_namespace.this.namespace_name
  base_capacity        = var.base_capacity
  publicly_accessible  = var.publicly_accessible
  enhanced_vpc_routing = var.enhanced_vpc_routing
  subnet_ids           = length(var.subnet_ids) == 0 ? null : var.subnet_ids
  security_group_ids   = length(var.security_group_ids) == 0 ? null : var.security_group_ids
  tags                 = merge(var.tags, { Name = var.workgroup_name })
}
