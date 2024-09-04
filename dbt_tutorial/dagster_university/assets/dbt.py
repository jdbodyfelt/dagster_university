import json

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import (
    dbt_assets, DbtCliResource, DagsterDbtTranslator
)

from ..project import dbt_project
from ..partitions import daily_partition

INCREMENTAL_SELECTOR = "config.materialized:incremental"

#/********************************************************************/

class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    """
    A customized dbt translator that overrides the get_asset_key method 
    to return a custom asset key for dbt sources.
    """
    def get_asset_key(self, dbt_resource_props):
        """
        Get the asset key for a dbt resource.
        
        Args:
            dbt_resource_props (dict): The properties of the dbt resource.
        
        Returns:
            AssetKey: The asset key.
        """
        rtype = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        if rtype == "source":
            key_ = AssetKey(f"taxi_{name}")
        else:
            key_ = super().get_asset_key(dbt_resource_props)
        return key_
    
    def get_group_name(self, dbt_resource_props):
        return dbt_resource_props["fqn"][1]

#/********************************************************************/
@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    exclude=INCREMENTAL_SELECTOR
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    """"""
    yield from dbt.cli(["build"], context=context).stream()

#/********************************************************************/
@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    select=INCREMENTAL_SELECTOR,
    partitions_def=daily_partition
)
def incremental_dbt_models(
    context: AssetExecutionContext,
    dbt: DbtCliResource
):
    time_window = context.partition_time_window
    dbt_vars = {
        "min_date": time_window.start.strftime('%Y-%m-%d'),
        "max_date": time_window.end.strftime('%Y-%m-%d')
    }
    yield from dbt.cli(["build", "--vars", json.dumps(dbt_vars)], context=context).stream()
