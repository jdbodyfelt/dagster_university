from dagster import AssetSelection, define_asset_job
from ..partitions import monthly_partition, weekly_partition

trips_by_week = AssetSelection.assets("trips_by_week")

trip_update_job = define_asset_job(
    name="trip_update_job",
    selection=AssetSelection.all() - AssetSelection.assets(["trips_by_week"]),
    partitions_def=monthly_partition
)

weekly_update_job = define_asset_job(
    name="weekly_update_job",
    selection=trips_by_week,
    partitions_def=weekly_partition
)