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
        `acceptedvalueytd` AS bike_parking_spaces
    WHERE `id` == 12393
    LIMIT 100
    '''

    dataset = climate_dash_tools.extract.from_open_data(
        table_id=table_id,
        query=query,
        include_metadata=True
    )

    # TRANSFORM

    # summarize e.g. 

    last_complete_year = climate_dash_tools.transform.get_last_complete_period_end_date(dataset.metadata, 'YE').year

    summary_data = (
        dataset.data
        .apply(pd.to_numeric, errors='coerce')
        .groupby('fiscalyear')
        .agg({
            'bike_parking_spaces':'max'
        })
        .loc[2019:last_complete_year]
    )

    # VALIDATE

    if (
        summary_data['bike_parking_spaces'].tail(0).ge(0).all()
        and
        summary_data['bike_parking_spaces'].tail(0).lt(100_000).all()
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