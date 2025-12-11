def run(): 
    import pathlib
    from io import BytesIO

    import pandas as pd
    import geopandas as gpd
    import requests

    import climate_dash_tools.extract
    import climate_dash_tools.transform
    import climate_dash_tools.logging_config

    pipeline_name = pathlib.Path(__file__).stem

    # set up logging
    logger = climate_dash_tools.logging_config.setup_logging_for_pipeline(pipeline_name)

    data_dir = pathlib.Path('Data')

    # EXTRACT

    # Get sample sites locations

    sites_url = 'https://data.cityofnewyork.us/api/views/bkwf-xfky/files/e93e4856-95f7-48d4-b4c0-fa54989cdbfc?download=true&filename=OpenData_Distribution_Water_Quality_Sampling_Sites_Updated_2021-0618.xlsx'

    r = requests.get(sites_url)

    try:
        r.raise_for_status()

        sites = pd.read_excel(BytesIO(r.content))

        sites = (
            sites
            .rename(columns=lambda col: (
                col
                .replace(' ','_')
                .replace('_-_','_')
                .lower()
            ))
            .dropna(subset='x_coordinate')
            .drop_duplicates(subset='sample_site')
        )


        sites_geo = gpd.GeoDataFrame(
            data=sites,
            geometry=gpd.points_from_xy(
                sites['x_coordinate'],
                sites['y_coordinate'],
                crs=2263
            )
        )

        logger.debug('Read sample sites locations from OpenData source')

        sites_geo.to_file(data_dir / 'Source Data' / 'water_quality_sample_locations.geojson')

        logger.debug('Saved cache sample sites locations')

    except Exception as e:

        sites_geo = gpd.read_file(data_dir / 'Source Data' / 'water_quality_sample_locations.geojson')

        logger.warning('Could not get sample sites locations from OpenData. Using cached data.')
        logger.info('The error on attempt to read new locations from OpenData was: %s', e)

    # Get samples

    table_id = 'bkwf-xfky'

    water_quality_samples = climate_dash_tools.extract.from_open_data(table_id)

    # Merge

    most_recent_sample = (
        water_quality_samples
        .sort_values('sample_date')
        .drop_duplicates(subset='sample_site',keep='last')
    )

    most_recent_sample_geo = (
        sites_geo
        .merge(
            most_recent_sample,
            on='sample_site',
            how='inner',
            validate='1:1'
        )
        [[
            'sample_station_(ss)_location_description',
            'sample_date' ,
            'sample_class',
            'residual_free_chlorine_mg_l',
            'turbidity_ntu',
            'coliform_quanti_tray_mpn_100ml',
            'e_coli_quanti_tray_mpn_100ml',
            'fluoride_mg_l',
            'geometry'
        ]]
    )

    # VALIDATE

    if not (
        most_recent_sample_geo.shape[0] > 0
    ):
        raise ValueError("Data validation failed: No rows of data")

    # SAVE
    data_dir.mkdir(exist_ok=True, parents=True)

    most_recent_sample_geo.to_file(
        data_dir / 'Summary Data' / f'{pipeline_name}.geojson'
    )

    return most_recent_sample_geo

if __name__ == "__main__":
    run()