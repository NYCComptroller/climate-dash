def run():
    import pathlib
    
    import climate_dash_tools.extract
    # import climate_dash_tools.transform
    import climate_dash_tools.logging_config

    pipeline_name = pathlib.Path(__file__).stem

    # set up logging
    logger = climate_dash_tools.logging_config.setup_logging_for_pipeline(pipeline_name)

    # EXTRACT

    table_id = 'w4pv-hbkt'

    query = '''
    SELECT
    CASE 
        WHEN `fuel_type` IN ('GAS', 'DIESEL') THEN 'GAS_AND_DIESEL'
        ELSE `fuel_type` 
    END AS `fuel_group`,
    COUNT(DISTINCT `vin`) AS `vehicle_count`
    WHERE
    `county` IN ("KINGS", "NEW YORK", "BRONX", "RICHMOND", "QUEENS")
    AND `record_type` = 'VEH' 
    AND `fuel_type` IN ("ELECTRIC", "GAS", "DIESEL")
    GROUP BY `fuel_group`
    '''

    vehicles = climate_dash_tools.extract.from_open_data(
        table_id=table_id,
        query=query,
        open_data_collection='state'
    )

    summary_data = (
        vehicles
        .assign(
            pct = vehicles['vehicle_count'] / vehicles['vehicle_count'].sum()
        )
        .set_index('fuel_group')
    )


    # VALIDATE

    if (
        summary_data['vehicle_count'].between(0,5_000_000).all()
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