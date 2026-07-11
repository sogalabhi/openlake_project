resource "azurerm_resource_group" "rg" {
  name     = "rg-openlake-dev"
  location = var.location
}

resource "azurerm_storage_account" "datalake" {
  name                     = "stopenlakeabhijith" # <-- CHANGE '12345' TO SOMETHING UNIQUE (lowercase/numbers only, max 24 chars)
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  
  # This is the magic flag that turns standard Blob Storage into ADLS Gen2
  is_hns_enabled           = true 
}

resource "azurerm_storage_data_lake_gen2_filesystem" "fs" {
  name               = "lakehouse"
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_mssql_server" "sqlserver" {
  name                         = "sql-openlake-abhijith" # <-- CHANGE '12345' TO MATCH YOUR UNIQUE NUMBER
  resource_group_name          = azurerm_resource_group.rg.name
  location                     = azurerm_resource_group.rg.location
  version                      = "12.0"
  administrator_login          = "sqladmin"
  administrator_login_password = "SuperSecretPassword123!" # In production, use KeyVault. For dev, this is fine.
}

resource "azurerm_mssql_database" "sqldb" {
  name           = "crm"
  server_id      = azurerm_mssql_server.sqlserver.id
  collation      = "SQL_Latin1_General_CP1_CI_AS"
  max_size_gb    = 2
  sku_name       = "Basic" # Keeps it extremely cheap for your $100 credit
}

resource "azurerm_mssql_firewall_rule" "allow_azure" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.sqlserver.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

resource "azurerm_mssql_firewall_rule" "allow_local" {
  name             = "AllowLocalIP"
  server_id        = azurerm_mssql_server.sqlserver.id
  start_ip_address = "49.43.243.42"
  end_ip_address   = "49.43.243.42"
}