def run(): 
    import pathlib

    import pandas
    import geopandas as gpd

    import climate_dash_tools.extract
    import climate_dash_tools.transform
    import climate_dash_tools.logging_config

    pipeline_name = pathlib.Path(__file__).stem

    # set up logging
    logger = climate_dash_tools.logging_config.setup_logging_for_pipeline(pipeline_name)

    # EXTRACT

    table_id = 'fc53-9hrv'

    query = '''
    SELECT
        agency,
        street,
        station_name,
        borough,
        community_district,
        type_of_charger,
        latitude,
        longitude
    WHERE latitude IS NOT NULL
    LIMIT 1000000
    '''

    chargers = climate_dash_tools.extract.from_open_data(table_id,query)

    chargers_geo = gpd.GeoDataFrame(
        data=chargers.drop(columns=['latitude','longitude']),
        geometry=gpd.points_from_xy(
            chargers['longitude'],
            chargers['latitude'],
            crs=4326
        )
    )

    # VALIDATE

    if (
        chargers_geo.shape[0] > 0
    ):

        # SAVE

        data_dir = pathlib.Path('Data/Summary Data')
        data_dir.mkdir(exist_ok=True, parents=True)

        chargers_geo.to_file(
            data_dir / f'{pipeline_name}.geojson'
        )

        return chargers_geo

    else:
        logger.error('Incorrect data: %s', chargers_geo.tail(20))

        return None

if __name__ == "__main__":
    run()