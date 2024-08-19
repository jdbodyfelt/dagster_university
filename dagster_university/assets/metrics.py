from dagster import asset

import plotly.express as px
import plotly.io as pio
import geopandas as gpd

import duckdb
import os

from . import constants

#/***********************************************************************/
@asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats() -> None:
    """
    The number of trips originating in each zone in Manhattan, with 
    geometry calcs on each frame. 
    """
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """

    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())

#/***********************************************************************/
@asset(
    deps=["manhattan_stats"],
)
def manhattan_map() -> None:
    """
    This asset generates a map of NYC, with each taxi zone colored by the
    number of trips originating in Manhattan.
    """
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig = px.choropleth_mapbox(trips_by_zone,
        geojson=trips_by_zone.geometry.__geo_interface__,
        locations=trips_by_zone.index,
        color='num_trips',
        color_continuous_scale='Plasma',
        mapbox_style='carto-positron',
        center={'lat': 40.758, 'lon': -73.985},
        zoom=11,
        opacity=0.7,
        labels={'num_trips': 'Number of Trips'}
    )
    pio.write_image(fig, constants.MANHATTAN_MAP_FILE_PATH)

#/***********************************************************************/
@asset(
    deps=["taxi_trips"]
)
def trips_by_week() -> None:
    """
    The number of trips per week aggregate. 
    """
    query = """
        create or replace table trips_by_week as (
            with raw as (
                select
                    date_trunc('day', pickup_datetime) as date_, 
                    date_part('weekday', pickup_datetime) as dow_, 
                    date_ - INTERVAL(dow_) DAY as period_,
                    date_trunc('day', period_) as period,
                    passenger_count,
                    total_amount,
                    trip_distance
                from
                    trips
            )
            select
                period, 
                count(1) as num_trips, 
                SUM(passenger_count) as passenger_count, 
                ROUND(SUM(total_amount),2) as total_amount, 
                round(sum(trip_distance),2) as trip_distance
            from raw
            group by 1
            order by 1
        );
    """
    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    conn.execute(query)
