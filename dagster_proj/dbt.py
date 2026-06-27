"""dbt ↔ Dagster integration.

Loads every dbt model/test as a Dagster asset (and asset check), so the full
bronze → silver → dbt staging → marts lineage shows in one asset graph and
`dbt build` runs under Dagster. dbt sources (`silver_*`) are remapped to the
silver asset keys so the graph connects to the upstream Dagster-built tables.
"""
import os
from pathlib import Path
from typing import Any, Mapping

import dagster as dg
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

from ingestion import config

DBT_DIR = Path(__file__).resolve().parent.parent / "dbt"

# dagster-dbt runs dbt from the project dir, so the profile's relative path won't
# resolve. Point it at the absolute lake file instead.
os.environ["TRANSITLAKE_DUCKDB"] = config.DUCKDB_PATH.as_posix()

dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
dbt_project.prepare_if_dev()  # regenerate manifest during `dagster dev`

dbt_resource = DbtCliResource(project_dir=dbt_project)


class TransitLakeDbtTranslator(DagsterDbtTranslator):
    """Map dbt sources to the matching silver asset keys so lineage connects."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            # source('silver', 'silver_routes') -> AssetKey(['silver_routes'])
            return dg.AssetKey([dbt_resource_props["name"]])
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=TransitLakeDbtTranslator(),
)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
