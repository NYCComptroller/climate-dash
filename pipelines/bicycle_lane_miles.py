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

    query = '''SELECT 
    `fiscalyear`,
    `indicator`,
    SUM(`acceptedvalue`) AS `total_miles`
    WHERE (`id` == 2851) OR (`id` == 12319)
    GROUP BY 
        `fiscalyear`,
        `indicator`
    '''

    bicycle_lane_miles = climate_dash_tools.extract.from_open_data(
        table_id=table_id,
        query=query,
        include_metadata=True
    )

    # TRANSFORM

    last_complete_year = climate_dash_tools.transform.get_last_complete_period_end_date(bicycle_lane_miles.metadata,'YE-JUN').year

    summary_data = (
        bicycle_lane_miles.data
        .set_index(['fiscalyear','indicator'])
        .unstack()
        ['total_miles']
        .assign(
            unprotected = lambda df: df['Bicycle lane miles installed'] - df['Bicycle lane miles installed — Protected']
        )
        .rename(columns={
            'Bicycle lane miles installed — Protected':'protected',

        })
        .loc[:last_complete_year]
        .filter(like=('protected'))
        .melt(
            ignore_index=False,
            value_name='miles',
        )
        .set_index('indicator',append=True)
        .sort_index()
    )

    # VALIDATE

    if (
        summary_data['miles'].between(0,300).all()
    ):

        # SAVE

        data_dir = pathlib.Path('Data/Summary Data')
        data_dir.mkdir(exist_ok=True)

        summary_data.to_csv(
            data_dir / f'{pipeline_name}.csv'
        )

        return summary_data

    else:
        logger.error('Incorrect data: %s', summary_data.tail(20))

        return None

if __name__ == "__main__":
    run()