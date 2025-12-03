# CKAN to ESS-DIVE notebook (with Tapis helpers)

This repository contains a ready-to-run Jupyter notebook that pulls datasets from the TACC CKAN instance, highlights missing metadata, stages resource files, and publishes metadata to the ESS-DIVE Dataset API. Tapis helpers are included so you can stage files to a Tapis Files system if you prefer not to download locally.

## What you can do
- Browse CKAN datasets (public or authenticated) and map fields to the ESS-DIVE payload format
- See which ESS-DIVE-required fields are missing before you submit
- Stage resources locally or upload them to a Tapis Files system
- Create the ESS-DIVE dataset (or run in dry-run mode to validate first)

## Prerequisites
- CKAN API key (only needed for private CKAN datasets)
- ESS-DIVE Dataset API token
- Optional: Tapis base URL, access token, and a Files system ID + target directory for staging
- Python 3.11+ with the packages in `.binder/environment.yaml` (or let the cookbook create the environment)

## Quick start (local Jupyter)
1. Create a conda env using `.binder/environment.yaml` and install `.binder/requirements.txt`.
2. Launch Jupyter Lab and open `notebook.ipynb`.
3. Fill in the widgets at the top:
   - CKAN URL/API key (or fetch a CKAN token via Tapis username/password — same flow used in `Ckan-metadata-netcdf`)
   - ESS-DIVE API base and token
   - Staging directory (local) and optional Tapis settings
4. Load CKAN datasets, select one, review missing metadata, and click **Validate and transfer**.
   - With `Dry-run` checked the notebook only validates and stages files.

## Running on TACC via the cookbook
The repo includes the standard cookbook scaffolding so you can run the notebook on TACC systems:
- `run.sh` points to this repository and creates a conda env named `ckan-essdive`.
- `app-cpu.json` / `app-gpu.json` can be imported into the Cookbook UI to register the app (update the container image if you build your own).
- Submit a job from the Cookbook UI, wait for the interactive session, and open Jupyter Lab to run the notebook as above.

## Repository layout
- `notebook.ipynb` — interactive workflow for CKAN → ESS-DIVE transfer
- `.binder/environment.yaml` and `.binder/requirements.txt` — dependencies for the notebook
- `run.sh` — bootstrap script used by the cookbook on TACC systems
- `app-*.json` — example Tapis app definitions for CPU/GPU images
