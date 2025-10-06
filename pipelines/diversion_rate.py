def run():
    import pandas as pd
    import pathlib

    import climate_dash_tools.extract
    import climate_dash_tools.transform
    import climate_dash_tools.logging_config

    pipeline_name = pathlib.Path(__file__).stem

    # set up logging
    logger = climate_dash_tools.logging_config.setup_logging_for_pipeline(pipeline_name)

    # EXTRACT

    table_id = 'ebb7-mvp5'

    query = '''
    SELECT
        `month`,
        `borough`,
        sum(`refusetonscollected`),
        sum(`papertonscollected`),
        sum(`mgptonscollected`),
        sum(`resorganicstons`),
        sum(`schoolorganictons`),
        sum(`leavesorganictons`),
        sum(`xmastreetons`),
        sum(`otherorganicstons`)
    GROUP BY
        `month`,
        `borough`
    LIMIT 100000000
    '''

    tonnage = climate_dash_tools.extract.from_open_data(table_id, query, include_metadata=True)

    # TRANSFORM

    # summarize e.g. 

    last_complete_fy = climate_dash_tools.transform.get_last_complete_period_end_date(tonnage.metadata, 'YE-JUN').year

    summary_data = (
        tonnage
        .data
        .fillna(0)
        .assign(
            fy = lambda row: (
                row['month']
                .apply(pd.to_datetime, format='%Y / %m')
                .apply(lambda date: pd.tseries.offsets.YearEnd(month=6).rollforward(date))
                .dt.year
            )
        )
        .drop(columns='month')
        .groupby([
            'fy',
            'borough'
        ])
        .sum()
        .assign(
            total_organics = lambda df: (
                df['sum_resorganicstons']
                + df['sum_schoolorganictons']
                + df['sum_leavesorganictons']
                + df['sum_xmastreetons']
                + df['sum_otherorganicstons']
            ),
            diversion_rate = lambda df: (
                (
                    df['total_organics']
                    + df['sum_mgptonscollected']
                    + df['sum_papertonscollected']
                )
                / (
                    df['sum_refusetonscollected']
                    + df['total_organics']
                    + df['sum_mgptonscollected']
                    + df['sum_papertonscollected']
                )
            )
        )
        .loc[
            2017:last_complete_fy
        ]
        ['diversion_rate']
        .unstack()
    )

    # VALIDATE

    if (
        (summary_data.gt(0) & summary_data.lt(1)).all().all()
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