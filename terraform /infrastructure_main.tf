provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "finance_rg" {
  name     = "finance-rg"
  location = "eastus"
}

resource "azurerm_storage_account" "datalake" {
  name                     = "financedatalake"
  resource_group_name      = azurerm_resource_group.finance_rg.name
  location                 = azurerm_resource_group.finance_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_databricks_workspace" "finance_dbw" {
  name                = "finance-dbw"
  resource_group_name = azurerm_resource_group.finance_rg.name
  location            = azurerm_resource_group.finance_rg.location
  sku                 = "premium"
}
