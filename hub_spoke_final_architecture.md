# Hub & Spoke Architecture — Final Reference Guide
## Databricks + Azure DevOps

---

## 1. Your Framework — How It Works

```mermaid
flowchart TD
    CALLER["External trigger<br/>params: category, subcategory,<br/>begin_date, end_date, file_name"]
    CALLER --> JOB["Databricks Job<br/>(in Spoke DAB)"]
    JOB --> NB["Bronze Runner Notebook<br/>(thin wrapper in Spoke)"]
    NB --> FW["Hub Bronze Framework<br/>(from wheel)"]

    subgraph "Framework Steps (Hub Wheel)"
        FW --> S1["Step 1: Read falcon_config.csv<br/>(from Spoke repo, deployed with DAB)"]
        S1 --> S2["Step 2: Upsert rows into<br/>config table in Unity Catalog"]
        S2 --> S3["Step 3: Query config table<br/>using job params"]
        S3 --> S4["Step 4: Resolve files to load<br/>(apply date logic / last 7 days default)"]
        S4 --> S5["Step 5: Load each file<br/>to its target bronze table"]
    end

    style CALLER fill:#3498db,color:#fff
    style FW fill:#2c3e50,color:#fff
    style S5 fill:#27ae60,color:#fff
```

---

## 2. High-Level Architecture

```mermaid
graph TD
    subgraph HUB["🔵 Hub Repo: deltalake-hub"]
        H_CORE["Core Classes<br/>logging, env_vars, widget_manager"]
        H_BRONZE["Bronze Framework<br/>base_loader, delimited_loader,<br/>parquet_loader, metadata_resolver,<br/>load_tracker"]
        H_QC["Quality Checks & Alerts"]
        H_DDL["Config Table DDL<br/>file_config, load_tracker"]
        H_TMPL["CI/CD Templates<br/>for spokes"]
        H_WHEEL["📦 deltalake_hub-x.y.z.whl"]
        H_CORE --> H_WHEEL
        H_BRONZE --> H_WHEEL
        H_QC --> H_WHEEL
    end

    subgraph FEED["Azure Artifacts Feed"]
        WHL["deltalake_hub-1.2.0.whl"]
    end

    subgraph SP1["🟢 Spoke: LOB-Falcon"]
        F_CFG["configs/bronze/falcon_config.csv<br/>(rows for Falcon LOB)"]
        F_JOB["Databricks Job (DAB)<br/>passes params → notebook"]
        F_NB["Bronze Runner Notebook<br/>(thin wrapper)"]
        F_SIL["Silver transforms"]
        F_GOLD["Gold aggregations"]
        F_DOWN["Downstream loading"]
    end

    subgraph SP2["🟠 Spoke: LOB-Eagle"]
        E_CFG["configs/bronze/eagle_config.csv"]
        E_JOB["Databricks Job (DAB)"]
        E_NB["Bronze Runner Notebook"]
        E_SIL["Silver transforms"]
        E_GOLD["Gold aggregations"]
    end

    HUB -->|"Hub CI publishes"| FEED
    FEED -->|"pip install at job runtime"| SP1
    FEED -->|"pip install at job runtime"| SP2

    subgraph UC["Unity Catalog"]
        TBL["bronze_metadata.file_config<br/>(runtime config table)"]
    end

    F_CFG -->|"upserted at runtime"| TBL
    E_CFG -->|"upserted at runtime"| TBL

    subgraph DB["Databricks Workspaces"]
        DEV --> STG --> PRD
    end

    SP1 --> DEV
    SP2 --> DEV

    style HUB fill:#2c3e50,color:#fff
    style FEED fill:#f39c12,color:#fff
    style SP1 fill:#27ae60,color:#fff
    style SP2 fill:#e67e22,color:#fff
    style UC fill:#1a73e8,color:#fff
```

---

## 3. Repository Structures

### 🔵 Hub Repo

```
deltalake-hub/
├── azure-pipelines/
│   ├── hub-ci.yml                    # Build, test, publish wheel
│   ├── hub-release.yml               # Publish to Azure Artifacts
│   └── templates/
│       ├── spoke-ci-template.yml     # Reusable CI for all spokes
│       └── spoke-release-template.yml # Reusable release for all spokes
├── scripts/
│   ├── build_wheel.sh
│   ├── run_tests.sh
│   ├── publish_wheel.sh
│   └── deploy_bundle.sh              # Used by spokes
├── src/
│   └── deltalake_hub/
│       ├── __init__.py
│       ├── core/
│       │   ├── logging_utils.py
│       │   ├── environment_var.py
│       │   └── widget_manager.py
│       ├── bronze/
│       │   ├── base_loader.py        # Orchestrates all steps below
│       │   ├── delimited_loader.py
│       │   ├── parquet_loader.py
│       │   ├── metadata_resolver.py  # Reads CSV → upserts table → resolves files
│       │   └── load_tracker.py       # Tracks loaded files; drives "last 7 days" logic
│       └── quality/
│           ├── data_quality.py
│           └── alerts.py
├── ddl/
│   ├── file_config.sql               # Hub-owned table schema (run once per workspace)
│   └── load_tracker.sql
├── tests/
├── setup.py
└── README.md
```

### 🟢 Spoke Repo (Falcon LOB)

```
deltalake-spoke-falcon/
├── azure-pipelines/
│   ├── ci.yml                        # Extends Hub CI template
│   └── release.yml                   # Extends Hub release template (dev→staging→prod)
├── databricks.yml                    # DAB config
├── resources/
│   ├── falcon_bronze_job.yml         # Bronze job (params + task)
│   ├── falcon_silver_job.yml         # Silver job
│   └── falcon_gold_job.yml           # Gold job
├── configs/
│   └── bronze/
│       └── falcon_config.csv         # ✅ THE config — all file types for Falcon
│                                     # ❌ No falcon_delimited.yml / falcon_parquet.yml needed
├── src/
│   ├── bronze/
│   │   └── falcon_bronze_runner.py   # Thin wrapper — passes params to Hub BronzeLoader
│   ├── silver/
│   │   ├── falcon_clean.py
│   │   └── falcon_enrich.py
│   ├── gold/
│   │   └── falcon_aggregate.py
│   └── downstream/
│       └── falcon_export.py
├── tests/
├── requirements.txt                  # deltalake-hub==1.2.0
└── README.md
```

> [!IMPORTANT]
> `configs/bronze/falcon_delimited.yml` and `falcon_parquet.yml` are **NOT needed**.
> The single `falcon_config.csv` replaces them — file format is a column in the CSV.

---

## 4. Config File: `configs/bronze/falcon_config.csv`

This is the **source of truth** for what files Falcon loads, where they come from, and where they go.
The framework reads this and upserts into the config table on every job run.

```csv
lob,category,subcategory,file_pattern,source_path,file_format,delimiter,has_header,dest_catalog,dest_schema,dest_table,is_active
falcon,transactions,credit,transactions_*.csv,abfss://raw@storage.dfs.core.windows.net/falcon/txn/credit/,delimited,",",true,${catalog},falcon_bronze,credit_transactions_raw,true
falcon,transactions,debit,debit_*.csv,abfss://raw@storage.dfs.core.windows.net/falcon/txn/debit/,delimited,",",true,${catalog},falcon_bronze,debit_transactions_raw,true
falcon,events,clickstream,*.parquet,abfss://raw@storage.dfs.core.windows.net/falcon/events/,parquet,,false,${catalog},falcon_bronze,clickstream_raw,true
falcon,events,pageviews,pageviews_*.parquet,abfss://raw@storage.dfs.core.windows.net/falcon/pageviews/,parquet,,false,${catalog},falcon_bronze,pageview_raw,true
```

**The `file_format` column** tells the framework which loader to use — no separate file per format.
Adding a new file type = adding one new row to this CSV. ✅

---

## 5. Config Table DDL (Hub-Owned, Run Once)

```sql
-- deltalake-hub/ddl/file_config.sql
-- Hub team runs this once per workspace (dev/staging/prod)
CREATE TABLE IF NOT EXISTS {catalog}.bronze_metadata.file_config (
    config_id         STRING         GENERATED ALWAYS AS (uuid()),
    lob               STRING         NOT NULL,
    category          STRING         NOT NULL,
    subcategory       STRING         NOT NULL,
    file_pattern      STRING         NOT NULL,
    source_path       STRING         NOT NULL,
    file_format       STRING         NOT NULL,  -- 'delimited' or 'parquet'
    delimiter         STRING,                   -- NULL for parquet
    has_header        BOOLEAN,
    dest_catalog      STRING         NOT NULL,
    dest_schema       STRING         NOT NULL,
    dest_table        STRING         NOT NULL,
    is_active         BOOLEAN        DEFAULT true,
    updated_ts        TIMESTAMP      DEFAULT current_timestamp()
) USING DELTA;

-- deltalake-hub/ddl/load_tracker.sql
CREATE TABLE IF NOT EXISTS {catalog}.bronze_metadata.load_tracker (
    tracker_id        STRING         GENERATED ALWAYS AS (uuid()),
    lob               STRING,
    category          STRING,
    subcategory       STRING,
    file_name         STRING,
    source_path       STRING,
    dest_table        STRING,
    load_status       STRING,        -- 'SUCCESS', 'FAILED', 'QUARANTINED'
    load_timestamp    TIMESTAMP      DEFAULT current_timestamp(),
    batch_id          STRING,
    row_count         LONG
) USING DELTA;
```

---

## 6. How Parameters Flow

```mermaid
sequenceDiagram
    participant EXT as External Trigger<br/>(Manual / Scheduler)
    participant JOB as Databricks Job<br/>(DAB)
    participant NB as falcon_bronze_runner.py
    participant FW as Hub BronzeLoader<br/>(from wheel)
    participant UC as Unity Catalog

    EXT->>JOB: Run with params:<br/>category=falcon<br/>subcategory=transactions<br/>begin_date=2024-01-01
    JOB->>NB: base_parameters inject into widgets:<br/>category, subcategory, begin_date, end_date, file_name
    NB->>NB: WidgetManager reads all params
    NB->>FW: BronzeLoader(config_path, params)
    FW->>FW: Read falcon_config.csv
    FW->>UC: MERGE INTO file_config<br/>(upsert CSV rows)
    FW->>UC: SELECT * FROM file_config<br/>WHERE category='falcon'<br/>AND subcategory='transactions'
    UC-->>FW: Returns matching config rows
    FW->>FW: Apply date filter<br/>(default: last 7 days unloaded)
    FW->>FW: Load each file using<br/>DelimitedLoader / ParquetLoader
    FW->>UC: INSERT INTO load_tracker
```

---

## 7. Spoke Bronze Runner (Thin Wrapper)

```python
# deltalake-spoke-falcon/src/bronze/falcon_bronze_runner.py
from deltalake_hub.bronze.base_loader import BronzeLoader
from deltalake_hub.core.widget_manager import WidgetManager

widgets = WidgetManager()

loader = BronzeLoader(
    spark       = spark,
    config_path = widgets.get("config_path"),   # ./configs/bronze/falcon_config.csv
    params = {
        "category":    widgets.get("category"),
        "subcategory": widgets.get("subcategory", default=None),
        "file_name":   widgets.get("file_name",   default=None),
        "begin_date":  widgets.get("begin_date",  default=None),
        "end_date":    widgets.get("end_date",     default=None),
    }
)

loader.run()
```

---

## 8. DAB Config (`databricks.yml`)

```yaml
# deltalake-spoke-falcon/databricks.yml
bundle:
  name: falcon-spoke

workspace:
  root_path: /Workspace/.bundle/${bundle.name}/${bundle.target}

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-dev-xxxxx.azuredatabricks.net
    variables:
      catalog: dev_catalog
      schema_prefix: falcon

  staging:
    workspace:
      host: https://adb-stg-xxxxx.azuredatabricks.net
    variables:
      catalog: staging_catalog
      schema_prefix: falcon

  prod:
    mode: production
    workspace:
      host: https://adb-prd-xxxxx.azuredatabricks.net
    run_as:
      service_principal_name: "sp-falcon-prod"
    variables:
      catalog: prod_catalog
      schema_prefix: falcon
```

---

## 9. Job Resource with Parameter Wiring

```yaml
# deltalake-spoke-falcon/resources/falcon_bronze_job.yml
resources:
  jobs:
    falcon_bronze_loader:
      name: "falcon-bronze-loader-${bundle.target}"
      # No schedule — triggered externally

      # ── Job-level parameters (passed by caller at trigger time) ──
      parameters:
        - name: category
          default: "falcon"    # Always falcon for this spoke; caller can override
        - name: subcategory
          default: ""          # Empty = all subcategories
        - name: file_name
          default: ""          # Empty = all files matching date filter
        - name: begin_date
          default: ""          # Empty = last 7 days unloaded files
        - name: end_date
          default: ""

      tasks:
        - task_key: bronze_load
          notebook_task:
            notebook_path: ./src/bronze/falcon_bronze_runner.py
            # ── Wire job params into notebook widgets (base_parameters) ──
            base_parameters:
              category:    "{{job.parameters.category}}"
              subcategory: "{{job.parameters.subcategory}}"
              file_name:   "{{job.parameters.file_name}}"
              begin_date:  "{{job.parameters.begin_date}}"
              end_date:    "{{job.parameters.end_date}}"
              config_path: "./configs/bronze/falcon_config.csv"  # Hardcoded — spoke-internal
          libraries:
            - pypi:
                package: deltalake-hub==1.2.0
                repo: https://pkgs.dev.azure.com/yourorg/_packaging/deltalake-artifacts/pypi/simple/
          job_cluster_key: bronze_cluster

        - task_key: silver_clean
          depends_on: [{task_key: bronze_load}]
          notebook_task:
            notebook_path: ./src/silver/falcon_clean.py
          libraries:
            - pypi:
                package: deltalake-hub==1.2.0
                repo: https://pkgs.dev.azure.com/yourorg/_packaging/deltalake-artifacts/pypi/simple/
          job_cluster_key: transform_cluster

        - task_key: gold_aggregate
          depends_on: [{task_key: silver_clean}]
          notebook_task:
            notebook_path: ./src/gold/falcon_aggregate.py
          job_cluster_key: transform_cluster

        - task_key: downstream_export
          depends_on: [{task_key: gold_aggregate}]
          notebook_task:
            notebook_path: ./src/downstream/falcon_export.py
          job_cluster_key: transform_cluster

      job_clusters:
        - job_cluster_key: bronze_cluster
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            num_workers: 2
            node_type_id: "Standard_DS3_v2"
            init_scripts:
              - workspace:
                  destination: /Workspace/init_scripts/configure_pip.sh  # Auth to Azure Artifacts
        - job_cluster_key: transform_cluster
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            num_workers: 4
            node_type_id: "Standard_DS4_v2"
```

---

## 10. Hub Shell Scripts

### build_wheel.sh
```bash
#!/bin/bash
set -euo pipefail
echo "=== Building Hub Wheel ==="
rm -rf dist/ build/ src/*.egg-info
pip install --upgrade build
python -m build --wheel --outdir dist/
echo "✅ Built: $(ls dist/*.whl)"
```

### run_tests.sh
```bash
#!/bin/bash
set -euo pipefail
RESULTS="${1:-./test-results}"
mkdir -p "$RESULTS"
python -m ruff check src/ tests/ || { echo "❌ Lint failed"; exit 1; }
python -m pytest tests/ \
  --junitxml="$RESULTS/test-results.xml" \
  --cov=deltalake_hub --cov-report=term-missing -v
echo "✅ All tests passed"
```

### publish_wheel.sh
```bash
#!/bin/bash
set -euo pipefail
FEED_URL="${1:?Feed URL required}"
TOKEN="${2:?Token required}"
pip install twine
twine upload --repository-url "$FEED_URL" \
  --username build --password "$TOKEN" dist/*.whl
echo "✅ Published"
```

### deploy_bundle.sh
```bash
#!/bin/bash
set -euo pipefail
TARGET="${1:?Target required}"
BUNDLE_PATH="${2:-.}"
export DATABRICKS_HOST="${3:?Host required}"
export DATABRICKS_TOKEN="${4:?Token required}"

if ! command -v databricks &>/dev/null; then
  curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
fi

pip install -r "$BUNDLE_PATH/requirements.txt"
databricks bundle validate --target "$TARGET" -p "$BUNDLE_PATH"
databricks bundle deploy  --target "$TARGET" -p "$BUNDLE_PATH"
echo "✅ Deployed to $TARGET"
```

---

## 11. Hub CI/CD Pipelines

### Hub CI (Build + Test)
```yaml
# azure-pipelines/hub-ci.yml
trigger:
  branches:
    include: [main, develop, feature/*]
  paths:
    include: [src/**, tests/**, setup.py]

pool:
  vmImage: 'ubuntu-latest'

stages:
  - stage: Build
    jobs:
      - job: BuildAndTest
        steps:
          - task: UsePythonVersion@0
            inputs: {versionSpec: '3.11'}
          - script: pip install --upgrade pip && pip install -r requirements-dev.txt && pip install -e .
            displayName: 'Install deps'
          - script: python -m ruff check src/ tests/
            displayName: 'Lint'
          - script: |
              python -m pytest tests/ \
                --junitxml=$(Build.ArtifactStagingDirectory)/test-results.xml \
                --cov=deltalake_hub --cov-report=xml:$(Build.ArtifactStagingDirectory)/coverage.xml -v
            displayName: 'Test'
          - task: PublishTestResults@2
            inputs:
              testResultsFiles: '$(Build.ArtifactStagingDirectory)/test-results.xml'
            condition: always()
          - script: |
              pip install build
              python -m build --wheel --outdir $(Build.ArtifactStagingDirectory)/dist
            displayName: 'Build Wheel'
          - task: PublishPipelineArtifact@1
            inputs:
              targetPath: '$(Build.ArtifactStagingDirectory)/dist'
              artifactName: 'hub-wheel'
```

### Hub Release (Publish to Azure Artifacts)
```yaml
# azure-pipelines/hub-release.yml
trigger: none

resources:
  pipelines:
    - pipeline: hub-ci
      source: 'Hub-CI-Pipeline'
      trigger:
        branches:
          include: [main]

pool:
  vmImage: 'ubuntu-latest'

stages:
  - stage: Publish
    jobs:
      - job: PublishWheel
        steps:
          - task: DownloadPipelineArtifact@2
            inputs:
              source: 'specific'
              pipeline: 'hub-ci'
              artifact: 'hub-wheel'
              path: '$(Pipeline.Workspace)/wheel'
          - task: UsePythonVersion@0
            inputs: {versionSpec: '3.11'}
          - script: |
              pip install twine
              twine upload \
                --repository-url https://pkgs.dev.azure.com/$(System.CollectionUri)/_packaging/deltalake-artifacts/pypi/upload/ \
                --username build --password $(System.AccessToken) \
                $(Pipeline.Workspace)/wheel/*.whl
            displayName: 'Publish to Azure Artifacts'
          - checkout: self
            persistCredentials: true
          - script: |
              VERSION=$(python setup.py --version)
              git tag "v${VERSION}" && git push origin "v${VERSION}"
            displayName: 'Tag Release'
```

---

## 12. Hub CI/CD Templates (Reusable by Spokes)

### Spoke CI Template
```yaml
# azure-pipelines/templates/spoke-ci-template.yml
parameters:
  - name: spokeName
    type: string
  - name: feedName
    type: string
    default: 'deltalake-artifacts'

stages:
  - stage: Build
    displayName: 'Build & Test ${{ parameters.spokeName }}'
    jobs:
      - job: BuildAndTest
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - task: UsePythonVersion@0
            inputs: {versionSpec: '3.11'}
          - task: PipAuthenticate@1
            inputs:
              artifactFeeds: '${{ parameters.feedName }}'
          - script: pip install --upgrade pip && pip install -r requirements.txt
            displayName: 'Install deps (incl. hub wheel)'
          - script: python -m ruff check src/ tests/
            displayName: 'Lint'
          - script: |
              python -m pytest tests/ \
                --junitxml=$(Build.ArtifactStagingDirectory)/test-results.xml -v
            displayName: 'Tests'
          - task: PublishTestResults@2
            inputs:
              testResultsFiles: '$(Build.ArtifactStagingDirectory)/test-results.xml'
              testRunTitle: '${{ parameters.spokeName }} Tests'
            condition: always()
          - script: |
              curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
              databricks bundle validate --target dev
            displayName: 'Validate DAB'
          - task: PublishPipelineArtifact@1
            inputs:
              targetPath: '$(Build.SourcesDirectory)'
              artifactName: 'spoke-${{ parameters.spokeName }}'
```

### Spoke Release Template
```yaml
# azure-pipelines/templates/spoke-release-template.yml
parameters:
  - name: targetEnvironment
    type: string
  - name: spokeName
    type: string
  - name: databricksHost
    type: string
  - name: databricksTokenSecret
    type: string
  - name: feedName
    type: string
    default: 'deltalake-artifacts'
  - name: adoEnvironment
    type: string
    default: ''

stages:
  - stage: Deploy_${{ parameters.targetEnvironment }}
    displayName: 'Deploy ${{ parameters.spokeName }} → ${{ parameters.targetEnvironment }}'
    jobs:
      - deployment: Deploy
        environment: ${{ coalesce(parameters.adoEnvironment, parameters.targetEnvironment) }}
        pool:
          vmImage: 'ubuntu-latest'
        strategy:
          runOnce:
            deploy:
              steps:
                - task: UsePythonVersion@0
                  inputs: {versionSpec: '3.11'}
                - task: PipAuthenticate@1
                  inputs:
                    artifactFeeds: '${{ parameters.feedName }}'
                - task: DownloadPipelineArtifact@2
                  inputs:
                    artifact: 'spoke-${{ parameters.spokeName }}'
                    path: '$(Pipeline.Workspace)/spoke'
                - script: |
                    cd $(Pipeline.Workspace)/spoke
                    pip install -r requirements.txt
                    curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
                    databricks bundle deploy --target ${{ parameters.targetEnvironment }}
                  displayName: 'Deploy → ${{ parameters.targetEnvironment }}'
                  env:
                    DATABRICKS_HOST: ${{ parameters.databricksHost }}
                    DATABRICKS_TOKEN: $(${{ parameters.databricksTokenSecret }})
```

---

## 13. Spoke CI and Release Pipelines

### Spoke CI (references Hub template)
```yaml
# deltalake-spoke-falcon/azure-pipelines/ci.yml
trigger:
  branches:
    include: [main, develop, feature/*]
  paths:
    include: [src/**, configs/**, tests/**, databricks.yml, requirements.txt]

pr:
  branches:
    include: [main, develop]

resources:
  repositories:
    - repository: hub
      type: git
      name: 'YourProject/deltalake-hub'
      ref: refs/tags/v1.2.0     # Pin to stable Hub version

extends:
  template: azure-pipelines/templates/spoke-ci-template.yml@hub
  parameters:
    spokeName: 'falcon'
    feedName: 'deltalake-artifacts'
```

### Spoke Release (Dev → Staging → Prod)
```yaml
# deltalake-spoke-falcon/azure-pipelines/release.yml
trigger:
  branches:
    include: [main]

pool:
  vmImage: 'ubuntu-latest'

resources:
  repositories:
    - repository: hub
      type: git
      name: 'YourProject/deltalake-hub'
      ref: refs/tags/v1.2.0
  pipelines:
    - pipeline: falcon-ci
      source: 'Falcon-CI-Pipeline'
      trigger:
        branches:
          include: [main]

variables:
  - group: databricks-secrets

stages:
  # ── Build & Validate ──────────────────────
  - stage: Build
    jobs:
      - job: BuildAndTest
        steps:
          - task: UsePythonVersion@0
            inputs: {versionSpec: '3.11'}
          - task: PipAuthenticate@1
            inputs:
              artifactFeeds: 'deltalake-artifacts'
          - script: pip install -r requirements.txt
            displayName: 'Install deps'
          - script: python -m pytest tests/ -v
            displayName: 'Run tests'
          - script: |
              curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
              databricks bundle validate --target dev
            displayName: 'Validate DAB'
          - task: PublishPipelineArtifact@1
            inputs:
              targetPath: '$(Build.SourcesDirectory)'
              artifactName: 'spoke-falcon'

  # ── Deploy to Dev (automatic) ─────────────
  - stage: DeployDev
    displayName: '🛠 Deploy to Dev'
    dependsOn: Build
    jobs:
      - deployment: DeployDev
        environment: 'databricks-dev'
        strategy:
          runOnce:
            deploy:
              steps:
                - task: UsePythonVersion@0
                  inputs: {versionSpec: '3.11'}
                - task: PipAuthenticate@1
                  inputs:
                    artifactFeeds: 'deltalake-artifacts'
                - task: DownloadPipelineArtifact@2
                  inputs:
                    artifact: 'spoke-falcon'
                    path: '$(Pipeline.Workspace)/spoke'
                - script: |
                    cd $(Pipeline.Workspace)/spoke
                    pip install -r requirements.txt
                    curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
                    databricks bundle deploy --target dev
                  env:
                    DATABRICKS_HOST: $(DATABRICKS_HOST_DEV)
                    DATABRICKS_TOKEN: $(DATABRICKS_TOKEN_DEV)

  # ── Deploy to Staging (approval gate) ─────
  - stage: DeployStaging
    displayName: '🧪 Deploy to Staging'
    dependsOn: DeployDev
    jobs:
      - deployment: DeployStaging
        environment: 'databricks-staging'
        strategy:
          runOnce:
            deploy:
              steps:
                - task: UsePythonVersion@0
                  inputs: {versionSpec: '3.11'}
                - task: PipAuthenticate@1
                  inputs:
                    artifactFeeds: 'deltalake-artifacts'
                - task: DownloadPipelineArtifact@2
                  inputs:
                    artifact: 'spoke-falcon'
                    path: '$(Pipeline.Workspace)/spoke'
                - script: |
                    cd $(Pipeline.Workspace)/spoke
                    pip install -r requirements.txt
                    curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
                    databricks bundle deploy --target staging
                  env:
                    DATABRICKS_HOST: $(DATABRICKS_HOST_STG)
                    DATABRICKS_TOKEN: $(DATABRICKS_TOKEN_STG)

  # ── Deploy to Production (approval gate) ──
  - stage: DeployProd
    displayName: '🚀 Deploy to Production'
    dependsOn: DeployStaging
    jobs:
      - deployment: DeployProd
        environment: 'databricks-prod'
        strategy:
          runOnce:
            deploy:
              steps:
                - task: UsePythonVersion@0
                  inputs: {versionSpec: '3.11'}
                - task: PipAuthenticate@1
                  inputs:
                    artifactFeeds: 'deltalake-artifacts'
                - task: DownloadPipelineArtifact@2
                  inputs:
                    artifact: 'spoke-falcon'
                    path: '$(Pipeline.Workspace)/spoke'
                - script: |
                    cd $(Pipeline.Workspace)/spoke
                    pip install -r requirements.txt
                    curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
                    databricks bundle deploy --target prod
                  env:
                    DATABRICKS_HOST: $(DATABRICKS_HOST_PRD)
                    DATABRICKS_TOKEN: $(DATABRICKS_TOKEN_PRD)
```

---

## 14. Azure Artifacts Auth (Cluster Startup)

```bash
# /Workspace/init_scripts/configure_pip.sh
# Uploaded to workspace once; referenced in job cluster init_scripts
PAT=$(databricks secrets get-bytes --scope ado-artifacts --key ado-pat)
pip config set global.index-url \
  "https://build:${PAT}@pkgs.dev.azure.com/yourorg/_packaging/deltalake-artifacts/pypi/simple/"
pip config set global.extra-index-url "https://pypi.org/simple/"
```

---

## 15. ADO Setup Checklist

| Item | Location in ADO | Action |
|---|---|---|
| **Artifacts Feed** | ADO → Artifacts | Create `deltalake-artifacts` feed |
| **Variable Group** | Pipelines → Library | `databricks-secrets` with `DATABRICKS_HOST_DEV/STG/PRD` and `DATABRICKS_TOKEN_DEV/STG/PRD` |
| **Environments** | Pipelines → Environments | `databricks-dev`, `databricks-staging`, `databricks-prod` |
| **Approval Gates** | Environment → Approvals & Checks | Add approvers for staging and prod environments |
| **Branch Policies** | Repos → Branches → Policies | Require CI to pass before PR merge to main/develop |
| **Databricks Secret Scope** | Databricks workspace | `ado-artifacts` scope with `ado-pat` key for Azure Artifacts auth |

---

## 16. End-to-End CI/CD Flow

```mermaid
flowchart TD
    A["👨‍💻 Developer creates feature branch<br/>updates falcon_config.csv or src/"] --> B["Creates PR to develop"]

    subgraph "CI Pipeline"
        B --> C["pip install hub wheel<br/>from Azure Artifacts"]
        C --> D["Lint → Tests → Validate DAB"]
        D --> E{"All passed?"}
        E -->|No| F["❌ PR blocked"]
        E -->|Yes| G["✅ PR approved & merged"]
    end

    subgraph "Release Pipeline"
        G --> H["🛠 Auto deploy → Dev"]
        H --> I["🧪 Deploy → Staging 🔒 Approval"]
        I --> J["🚀 Deploy → Production 🔒 Approval"]
    end

    style F fill:#e74c3c,color:#fff
    style J fill:#27ae60,color:#fff
    style I fill:#f39c12,color:#fff
```
