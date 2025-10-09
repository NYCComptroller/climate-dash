def run():
    import pathlib
    
    import pandas as pd

    import climate_dash_tools.extract
    import climate_dash_tools.transform
    import climate_dash_tools.logging_config

    pipeline_name = pathlib.Path(__file__).stem

    # set up logging
    logger = climate_dash_tools.logging_config.setup_logging_for_pipeline(pipeline_name)

    # EXTRACT

    table_id = 'rbed-zzin'

    query = '''
    SELECT 
        `fiscalyear`,
        `acceptedvalueytd` AS electric_vehicles
    WHERE `id` == 10956
    '''

    electric_vehicles = climate_dash_tools.extract.from_open_data(
        table_id=table_id,
        query=query,
        include_metadata=True
    )

    # TRANSFORM

    last_complete_year = climate_dash_tools.transform.get_last_complete_period_end_date(electric_vehicles.metadata, 'YE').year

    summary_data = (
        electric_vehicles.data
        .apply(pd.to_numeric, errors='coerce')
        .groupby('fiscalyear')
        .agg({
            'electric_vehicles':'max'
        })
        .loc[:last_complete_year]
    )

    # VALIDATE

    if (
        summary_data['electric_vehicles'].tail(1).between(0,50_000).all()
    ):

        # SAVE

        data_dir = pathlib.Path('Data/Summary Data')
        data_dir.mkdir(exist_ok=True, parents=True)

        summary_data.to_csv(
            data_dir / f'{pipeline_name}.csv'
        )

        return summary_data

    else:
        logger.error('Incorrect data: %s', summary_data.tail(20))

        return None

if __name__ == "__main__":
    run()