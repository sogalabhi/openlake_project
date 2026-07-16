# Chapter 11: Infrastructure as Code - Terraform & Azure Core Resources

*← [Back to Index](./index.md)*

---

## 11.1 Why IaC At All? The Problem With Clicking

Before Terraform, provisioning cloud infrastructure meant logging into the Azure Portal and clicking: create a resource group, fill in a form, create a storage account, configure settings, create a database, set firewall rules. This works exactly once - and it breaks in every way that matters for engineering.

**The problems with manual portal clicking:**

- **Not reproducible** - if someone asks "how did you set this up?", the answer is "I clicked through a wizard." There's no record of what exactly was configured.
- **Not versionable** - you can't `git diff` your Azure Portal click history. If you made a mistake, you don't know when.
- **Not reviewable** - your team can't review a pull request that says "I changed the SKU in the portal." Changes are invisible.
- **Silently drifts** - if someone manually changes a setting in the portal, your infrastructure no longer matches your documentation. You have no idea.
- **Doesn't scale to multiple environments** - recreating a dev environment in staging means clicking through every form again, hoping you remember every setting.

IaC (Infrastructure as Code) solves all of this by treating infrastructure the same way you treat application code: text files that describe the desired state, committed to version control, reviewed in pull requests.

> [!TIP]
> **Interview Answer**: Manual portal configuration isn't reproducible, versionable, or reviewable. IaC defines infrastructure as text files in version control - the same discipline as application code. A new environment becomes `terraform apply`, not "remember which boxes to check." For a team, this also enables PR review of infrastructure changes, which is impossible with portal clicks.

---

## 11.2 Declarative vs. Imperative - The Core Mental Model

There are two ways to tell a computer to build something.

**Imperative** - you describe the steps: "Create a resource group. Then create a storage account inside it. Then enable HNS on the storage account."

This is how bash scripts and `az cli` sequences work:
```bash
az group create --name rg-openlake-dev --location westus3
az storage account create --name stopenlakeabhijith --resource-group rg-openlake-dev --sku Standard_LRS --kind StorageV2 --enable-hierarchical-namespace true
```

**Declarative** - you describe the desired end state: "There should exist a resource group and a storage account with HNS enabled." You don't say how to create it - the tool figures that out.

This is Terraform. You write what you want, and Terraform computes the steps needed to make reality match your declaration.

The practical consequence: if the storage account already exists, `terraform apply` doesn't re-create it - it compares the current state to your declaration and does nothing (or just updates what changed). An `az cli` script has no concept of "does this already exist" - it either errors or duplicates.

> [!TIP]
> **Interview Answer**: Declarative IaC describes the desired end state - "a storage account with HNS enabled should exist" - not the steps to create it. Terraform diffs your declaration against the current state and only applies the delta. Imperative scripts (az cli, bash) describe steps, not outcomes - they don't know what already exists and must be written defensively. Declarative is more maintainable at scale because changing desired state is a one-line edit; changing imperative steps requires understanding every side effect.

---

## 11.3 Why Terraform Over ARM Templates or Bicep?

If you're asked "why not use native Azure tools?":

| | Terraform | ARM Templates | Bicep |
|---|---|---|---|
| **Language** | HCL (clean, readable) | JSON (verbose, hard to read) | Bicep DSL (cleaner than ARM) |
| **Cloud support** | Multi-cloud (Azure, AWS, GCP, etc.) | Azure only | Azure only |
| **Module ecosystem** | Terraform Registry - huge, community-maintained | Limited | Growing but smaller |
| **State management** | Explicit, inspectable `.tfstate` | Managed by ARM internally | Managed by ARM internally |
| **First-party integration** | Third-party (but widely adopted as de facto standard) | Native Azure - deeper integration | Native Azure |

The honest answer for an interview: for a **solo Azure project**, Bicep would have been equally valid and arguably simpler. Terraform's advantage appears when: (a) you need multi-cloud or hybrid-cloud IaC, (b) you want to leverage the broader module ecosystem, or (c) your team already knows Terraform. For this project, using Terraform was also a deliberate portfolio choice - it's the industry-dominant IaC tool that appears in most data engineering job requirements.

---

## 11.4 The Three Files - What Goes Where and Why

This is a common interview follow-up. The split isn't arbitrary - each file has a distinct responsibility.

### [`providers.tf`](../infra/providers.tf) - The Plugin Declaration

```hcl
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
  skip_provider_registration = true
  subscription_id            = "bea46914-..."
}
```

**What it does**: Declares which cloud provider plugin Terraform should download and use. Without this, Terraform has no idea how to talk to Azure - it's provider-agnostic by default.

The `azurerm` provider is Terraform's plugin that translates HCL resource declarations into Azure REST API calls. When you write `resource "azurerm_storage_account"`, it's the provider that knows the exact Azure API endpoint to call, what JSON body to send, and how to interpret the response.

**Why it's separate from `main.tf`**: Provider configuration is infrastructure-wide boilerplate. Keeping it separate means `main.tf` can focus entirely on resources, and if you add a second provider (say, `hashicorp/random` to generate unique names), you add it here without cluttering the resource definitions.

### [`variables.tf`](../infra/variables.tf) - The Parameter Declarations

```hcl
variable "location" {
  type        = string
  default     = "westus3"
  description = "The target Azure region for all OpenLake resources"
}
```

**What it does**: Declares input variables - the knobs you can turn without editing resource logic. A variable has a type, an optional default, and a description.

**The key distinction**: `variables.tf` *declares* variables. Where you *set* their values:
- The `default` field in `variables.tf` is the fallback.
- A `terraform.tfvars` file (e.g., `location = "eastus"`) overrides the default.
- CLI flags (`terraform apply -var="location=eastus"`) override everything.

This project only has one variable (`location`) because most values are hardcoded for a solo dev setup. In a multi-environment project (dev/staging/prod), you'd have a `variables.tf` with many parameters and separate `dev.tfvars` / `prod.tfvars` files that supply different values for each environment.

### [`main.tf`](../infra/main.tf) - The Actual Resource Definitions

```hcl
resource "azurerm_resource_group" "rg" {
  name     = "rg-openlake-dev"
  location = var.location
}

resource "azurerm_storage_account" "datalake" {
  name                = "stopenlakeabhijith"
  resource_group_name = azurerm_resource_group.rg.name   # <-- reference
  location            = azurerm_resource_group.rg.location
  is_hns_enabled      = true
  ...
}
```

**What it does**: Declares the actual Azure resources to provision. Each `resource` block maps to a real Azure API resource.

Notice `resource_group_name = azurerm_resource_group.rg.name` - this is an **attribute reference**. The storage account references the resource group's name by reading it from the resource group block. This does two things: it passes the actual name as a value, and it tells Terraform that the storage account *depends on* the resource group. Terraform will create the resource group first, automatically, without you specifying the order.

**What's missing from this project (worth knowing)**: There's no `outputs.tf`. This file would expose computed values (like the storage account connection string, or the SQL server FQDN) for consumption by other systems - for example, automatically generating the values needed in `.env` instead of manually copy-pasting them from the Azure Portal.

> [!TIP]
> **Interview Answer**: Three files, three responsibilities. `providers.tf` declares the Azure plugin - how Terraform speaks Azure API. `variables.tf` declares parameters - what can be changed without touching resource logic. `main.tf` declares the actual resources. The split keeps each file focused. A missing piece in this project is `outputs.tf` - it would expose values like the storage account key so they can feed into `.env` automatically rather than being manually copied.

---

## 11.5 The State File - Terraform's Memory

This gets grilled in almost every infrastructure interview.

### What It Is

`terraform.tfstate` is a JSON file that records the mapping between your HCL declarations and the real Azure resource IDs that were created. After `terraform apply`, the state file knows: "the resource `azurerm_storage_account.datalake` corresponds to Azure resource ID `/subscriptions/bea46914.../storageAccounts/stopenlakeabhijith`."

```json
{
  "resources": [
    {
      "type": "azurerm_storage_account",
      "name": "datalake",
      "instances": [
        {
          "attributes": {
            "id": "/subscriptions/bea46914-4ae4.../storageAccounts/stopenlakeabhijith",
            "name": "stopenlakeabhijith",
            "is_hns_enabled": true,
            ...
          }
        }
      ]
    }
  ]
}
```

### How `terraform plan` Uses It

Every `terraform plan` diffs three things:
1. **Your HCL config** - what you want
2. **The state file** - what Terraform last created
3. **Live Azure** - what actually exists right now (via API refresh)

The diff between (1) and (2)+(3) tells Terraform what to create, update, or destroy.

### Local State - The Solo Dev Shortcut

This project uses **local state**: `infra/terraform.tfstate` sits on disk in the `infra/` directory. You can see it listed there (and the `.backup` files Terraform creates before each apply).

This is explicitly wrong for teams, and worth acknowledging in an interview:
- **No locking**: if two people run `terraform apply` simultaneously, they both read the same state file, make changes, and write conflicting results - corrupted state.
- **Not shared**: another developer cloning the repo doesn't have the state file (it's in `.gitignore`) and can't run Terraform at all.
- **Risk of loss**: if your disk fails and you haven't backed up the state file, Terraform has no memory of what it created. You'd have to manually import every resource.

### Remote State - The Production Answer

In production, state lives in a **remote backend** - for Azure, that's a blob container in Azure Storage:

```hcl
terraform {
  backend "azurerm" {
    resource_group_name  = "rg-terraform-state"
    storage_account_name = "stterraformstate"
    container_name       = "tfstate"
    key                  = "openlake.terraform.tfstate"
  }
}
```

This gives you:
- **Locking via blob lease**: Azure Storage's blob lease mechanism prevents two concurrent `apply` operations. The first to `apply` acquires a lease; the second blocks until the lease is released.
- **Shared access**: all team members and CI/CD pipelines read/write the same state.
- **Durability**: the state is replicated across Azure's storage infrastructure.

### State Drift

If someone manually changes a resource in the Azure Portal after Terraform created it - say, they upgraded the SQL tier from Basic to Standard via the portal - the state file still says Basic. The live resource says Standard. This is **state drift**.

`terraform plan` will detect this mismatch (because it refreshes live resource state) and show a diff. `terraform apply` will then revert the manual change back to match your HCL - which may or may not be what you want.

The lesson: once Terraform owns a resource, all changes should go through Terraform, not the portal, to avoid drift.

> [!TIP]
> **Interview Answer**: The state file is Terraform's memory - it maps HCL declarations to real Azure resource IDs. `terraform plan` diffs your config against the state (and live resources) to compute what needs to change. Local state is fine for a solo dev project but wrong for teams: no locking means two concurrent `apply` runs corrupt state, and the file isn't shared. Production state goes in a remote backend (an Azure Storage blob container) which provides locking via blob lease and shared access for all team members. Manual portal changes after Terraform creates a resource cause state drift - Terraform will detect and revert them on the next `apply`.

---

## 11.6 The init → plan → apply → destroy Lifecycle

```bash
terraform init     # Download providers, set up backend
terraform plan     # Show what will change (dry run, no changes made)
terraform apply    # Execute the plan (creates/modifies/destroys resources)
terraform destroy  # Tear down everything tracked in state
```

**`terraform init`** downloads the `azurerm` provider binary declared in `providers.tf` and stores it in `.terraform/`. This is why `.terraform/` is in `.gitignore` - it's a local binary cache, not source code.

**`terraform plan`** is safe - it makes no changes to Azure. It reads the state file, refreshes live resource state, diffs your config, and prints a summary: `+` for creates, `~` for updates, `-` for destroys. Always read this before applying.

**Idempotency**: running `terraform apply` twice with no config changes between runs should produce `0 to add, 0 to change, 0 to destroy` on the second run. This is a core guarantee - Terraform only makes changes when config diverges from reality.

---

## 11.7 Authentication - `az login` vs Service Principal

This project used **`az login`** - you ran a command, a browser opened, you logged in with your personal Microsoft account. Terraform picked up the resulting token automatically (the `azurerm` provider checks for Azure CLI credentials).

This is fine for solo dev. The question in an interview: **"How would this work in CI/CD?"**

A GitHub Actions pipeline has no browser. You can't `az login` interactively in an automated environment. The answer is a **Service Principal** - an application identity in Azure Active Directory with its own client ID and secret (or certificate or OIDC token), specifically designed for non-interactive authentication:

```bash
az ad sp create-for-rbac --name "sp-openlake-ci" --role Contributor \
  --scopes /subscriptions/bea46914-...
```

This gives you a `client_id`, `client_secret`, and `tenant_id`. In GitHub Actions, these become repository secrets, injected as environment variables at pipeline runtime:

```yaml
env:
  ARM_CLIENT_ID: ${{ secrets.AZURE_CLIENT_ID }}
  ARM_CLIENT_SECRET: ${{ secrets.AZURE_CLIENT_SECRET }}
  ARM_TENANT_ID: ${{ secrets.AZURE_TENANT_ID }}
  ARM_SUBSCRIPTION_ID: ${{ secrets.AZURE_SUBSCRIPTION_ID }}
```

The `azurerm` provider reads these environment variables automatically - no `az login` needed.

> [!TIP]
> **Interview Answer**: `az login` is interactive - fine for a solo dev, not for CI/CD where no human is present. In a pipeline, you authenticate using a Service Principal: an application identity in Azure AD with a client ID/secret. The SP credentials are stored as encrypted secrets in GitHub (or Key Vault), injected as environment variables at runtime, and the `azurerm` provider reads them automatically. This is also more secure: the SP can be given minimum-required permissions rather than inheriting a developer's full subscription access.

---

## 11.8 The Dependency Graph - How Terraform Knows the Order

Terraform doesn't need you to write `create resource group first, then storage account`. It figures this out automatically from attribute references.

In `main.tf`:
```hcl
resource "azurerm_storage_account" "datalake" {
  resource_group_name = azurerm_resource_group.rg.name  # references rg
  location            = azurerm_resource_group.rg.location
  ...
}
```

Because `datalake` reads attributes from `rg`, Terraform knows `rg` must exist before `datalake` can be created. This is an **implicit dependency** - inferred from attribute references.

Sometimes you need ordering without a direct attribute reference. That's an **explicit dependency**:

```hcl
resource "azurerm_role_assignment" "storage_blob_contributor" {
  ...
  depends_on = [azurerm_storage_account.datalake]
}
```

A concrete example from this project: RBAC role assignments. When Terraform creates a role assignment (granting Spark write access to the storage account), the role assignment technically doesn't reference the storage account's attributes in the resource block - but it absolutely must exist before any Spark job attempts a write. `depends_on` expresses this ordering.

There's also a timing subtlety: Azure RBAC propagation can take 30-60 seconds after Terraform creates the role assignment before the permission is actually enforced across Azure's global control plane. If Spark runs immediately after `terraform apply`, it might still hit a 403 even with a valid role assignment. The real-world fix is either a `time_sleep` resource (explicit wait) or a retry in the pipeline.

> [!TIP]
> **Interview Answer**: Terraform builds a Directed Acyclic Graph from resource dependencies. Implicit dependencies come from attribute references - if resource B reads an attribute of resource A, Terraform knows A must exist first. Explicit dependencies use `depends_on` for cases where there's no attribute reference but an ordering requirement still exists - like an RBAC role assignment that must precede a data write. There's also a real-world gotcha: Azure RBAC propagation can lag 30-60 seconds after a role assignment is created, causing 403 errors even on valid assignments in fast pipelines.

---

## 11.9 The RBAC 403 Bug - Control Plane vs. Data Plane

This is one of the strongest "real debugging story" moments in the whole project.

### What Happened

After `terraform apply` provisioned the storage account, running the Spark job to write to ADLS Gen2 failed with:
```
HTTP 403 Forbidden - This request is not authorized to perform this operation using this permission.
```

### The Root Cause - Two Separate Permission Systems

Azure has two distinct permission layers on every resource:

**Control Plane** - managed by Azure Resource Manager (ARM). Governs: can you see the resource in the portal? Can you delete it? Can you change its settings? Role: **Storage Account Contributor** or higher.

**Data Plane** - managed by the service itself (ADLS Gen2 in this case). Governs: can you actually read or write *data* inside the storage account? Role: **Storage Blob Data Contributor**.

Creating a resource (even as the owner of the subscription) does not automatically grant data-plane access via RBAC. These are separate permission grants. The storage account existed, and you could see it, manage it, delete it - but the Spark process's identity had no data-plane role assignment, so every attempt to read or write a blob returned 403.

### The Fix

Assign the data-plane role explicitly (either via Terraform or Azure Portal):
```hcl
resource "azurerm_role_assignment" "spark_storage_access" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = "<your-azure-ad-object-id>"
}
```

An alternative that bypasses RBAC entirely: use the **Storage Account Access Key** directly. This is simpler but has no fine-grained control, no per-identity audit trail, and the key must be rotated manually. RBAC with managed identities is the production recommendation.

> [!TIP]
> **Interview Answer**: Azure has two permission layers. The *control plane* (ARM) controls resource management - creating, deleting, configuring the resource. The *data plane* controls actual data access - reading and writing blobs. These are separate role assignments. `Storage Account Contributor` only grants control plane access; `Storage Blob Data Contributor` grants data plane access. Creating a resource doesn't automatically grant data access - I hit exactly this 403 when the Spark process couldn't write to the storage account despite having subscription-level access to manage the resource.

---

## 11.10 Resource Naming Constraints - The Real-World Friction

Real Azure resource names have strict constraints that you only discover by hitting errors:

| Resource | Constraint | This Project |
|---|---|---|
| Storage Account | Globally unique, 3-24 chars, lowercase letters and numbers only (no hyphens) | `stopenlakeabhijith` |
| SQL Server | Globally unique, lowercase, hyphens allowed | `sql-openlake-abhijith` |
| Resource Group | Unique within subscription | `rg-openlake-dev` |
| Databricks Workspace | Unique within subscription | `dbw-openlake-abhijith` |

The storage account name `stopenlakeabhijith` is globally unique (it's in Azure's global DNS namespace as `stopenlakeabhijith.blob.core.windows.net`). If someone else already has that name, you get a conflict error and must choose differently.

### Why `westus3`?

The region was chosen specifically to work around resource availability constraints. Azure SQL and certain VM families are not available in every region - particularly on free trial subscriptions. Attempting to provision in other regions hit quota or availability errors; `westus3` had the right combination of available resource families and proximity.

---

## 11.11 What You Didn't Use But Should Know Exists

These come up in "did you consider X" follow-up questions:

**Modules** - reusable, composable Terraform configurations. Like a function in programming: you write a module once that creates a storage account + filesystem + role assignments together, and call it multiple times for different environments. For a single-environment personal project, modules aren't needed. For dev/staging/prod, you'd modularize core resource groups so each environment is `module.openlake_dev { ... }`.

**Workspaces** - Terraform's built-in mechanism for managing multiple environments from the same configuration using different state files. `terraform workspace new staging` creates an isolated state namespace. Less flexible than separate variable files per environment, but built-in.

**`terraform.tfvars`** - a file where you assign values to variables declared in `variables.tf`. The difference:
- `variables.tf`: `variable "location" { type = string }` - the declaration
- `terraform.tfvars`: `location = "westus3"` - the assignment

This project put the default in `variables.tf` directly, which is fine for one environment.

**Secrets handling** - the SQL admin password in `main.tf` is hardcoded:
```hcl
administrator_login_password = "SuperSecretPassword123!"  # don't do this in production
```
This is a known shortcut for a dev project. In production: declare a `variable "sql_admin_password" { sensitive = true }`, set its value via environment variable (`TF_VAR_sql_admin_password`) or Key Vault, and never commit the value to git. The `sensitive = true` flag prevents the value from appearing in plan/apply output.

> [!TIP]
> **Interview Answer on hardcoded passwords**: "In this dev project, the SQL admin password is hardcoded in `main.tf` - which I know is wrong for production. The production pattern is to declare it as a sensitive variable, source it from Azure Key Vault or an environment variable, and never let it appear in version control or plan output. I made this tradeoff consciously for a solo dev project where the credentials are throwaway values."

---

## Summary: What Chapter 11 Answers

| Question | Short Answer |
|---|---|
| Why IaC over portal clicking? | Reproducible, versionable, reviewable, detects drift |
| Declarative vs imperative? | Declarative describes desired end state; Terraform computes steps. Imperative (bash/az cli) describes steps without knowing what exists |
| Why Terraform over Bicep/ARM? | Multi-cloud, HCL readability, module ecosystem; Bicep is a valid Azure-native alternative |
| What does `providers.tf` do? | Declares the Azure plugin that translates HCL → Azure REST API calls |
| What does `variables.tf` do? | Declares input parameters; `tfvars` assigns their values |
| What does `main.tf` do? | Declares the actual Azure resources to provision |
| What's the state file? | JSON mapping HCL declarations to real Azure resource IDs; Terraform's memory |
| Local vs remote state? | Local is a solo-dev shortcut - no locking, no sharing. Remote (Azure blob) enables team use via lease-based locking |
| What is state drift? | Manual portal changes after Terraform creates a resource; detected on next `plan`, reverted on `apply` |
| `az login` vs Service Principal? | `az login` is interactive (solo dev). SP is for CI/CD - non-interactive, scoped permissions |
| Control plane vs data plane (RBAC)? | Control plane = manage the resource. Data plane = read/write data inside it. Two separate role assignments |

---

*Next: [Chapter 12 - Architecture Decision: Option B & Protocol Layer](./ch12_architecture_decision.md)*
