# Databricks bundle — `e-source/IEDR-pipeline-bundles`

## Overview
This folder contains the Databricks bundle for the IEDR pipelines. Use the Databricks Bundle CLI to validate and deploy declarative pipelines, jobs and associated files to your Databricks workspace.

Path: `e-source/IEDR-pipeline-bundles`

## Prerequisites
- Databricks CLI / Bundle plugin installed and configured for the target account/profile.
  - Ensure you have a configured profile, e.g. `your-profile-name`.
  - Example configuration command (Databricks CLI v2): `databricks configure --token --profile "your-profile-name"`
- Network access to the Databricks workspace and a valid token.
- Project files organized in this bundle:
  - `resources/` — each resource YAML (jobs and pipelines)
  - `src/` — local directories and files to be uploaded to workspace

## Repository layout
- `resources/`
  - `iedr-task-orchestrator.job.yml`
  - `{utility}-transformation.pipeline.yml`
  - `and other resource YAML files`
- `src/` (uploaded to workspace when deploying)
    - `iedr-ingestion/`
    - `iedr-transformation/`
    - `sql/`
    - `and other supporting files`

Note: workspace entries in resource YAML must reference files (not directories). If you point to a directory, validation/deploy will fail.

## Validate bundle (safe check)
Run bundle validation to catch schema issues before deploy:

### Step 1: Navigate to the bundle directory
```bash
cd e-source/IEDR-pipeline-bundles 
```
### Step 2: Run validation command
```bash
databricks bundle validate --profile "your-profile-name"
```
This checks resource YAML files for schema correctness without making changes to the workspace.
### Step 3: Deploy the bundle
```bash
databricks bundle deploy --profile "your-profile-name"
```
If validation passes, you will see a success message. If there are errors, review the output to identify and fix issues in the resource YAML files.
This command deploys the resources defined in the bundle to the specified Databricks workspace.

#### Diagram for IEDR Pipeline workflow
![IEDR Pipeline Architecture](Screenshot%202026-01-19%20at%208.59.18.png)
