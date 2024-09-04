from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator

from ..project import dbt_project


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

       

@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator()
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    """"""
    yield from dbt.cli(["build"], context=context).stream()
    
