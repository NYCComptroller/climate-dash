def run():
    import pathlib
    
    import climate_dash_tools.extract
    import climate_dash_tools.transform
    import climate_dash_tools.logging_config

    pipeline_name = pathlib.Path(__file__).stem

    # set up logging
    logger = climate_dash_tools.logging_config.setup_logging_for_pipeline(pipeline_name)


    table_id = 'wgsj-jt5f'

    ### Step 1: Get total per year

    # EXTRACT

    query = '''
    SELECT
        date_extract_y(`interconnection_date`) AS `year`,
        sum(`estimated_pv_system_size`) / 1000 AS `total_installed_mw`
    WHERE
        caseless_one_of(
        `county`,
        "Bronx",
        "New York",
        "Kings",
        "Queens",
        "Richmond"
        )
    GROUP BY date_extract_y(`interconnection_date`)
    '''

    installed_mw_by_year = climate_dash_tools.extract.from_open_data(
        table_id=table_id,
        query=query,
        open_data_collection='state',
        include_metadata=True
    )

    ### Step 2: Get end of last complete year 

    end_date_of_last_complete_year = climate_dash_tools.transform.get_last_complete_period_end_date(
        installed_mw_by_year.metadata, 
        'YE'
    )

    last_complete_year = end_date_of_last_complete_year.year

    # Step 3: Get remaining to goal, as of end of last complete year

    query = f'''SELECT 
        sum(`estimated_pv_system_size`) / 1000 AS `installed`,
        1000 - sum(`estimated_pv_system_size`) / 1000 AS `remaining`
    WHERE
    caseless_one_of(
        `county`,
        'Bronx',
        'New York',
        'Kings',
        'Queens',
        'Richmond'
    )
    AND (`interconnection_date` >= '2014-01-01T00:00:00' :: floating_timestamp)
    AND (`interconnection_date` <= '{end_date_of_last_complete_year.isoformat()}' :: floating_timestamp)
    '''

    installed_remaining = climate_dash_tools.extract.from_open_data(
        table_id,query,
        open_data_collection='state'
    )


    # TRANSFORMS

    summary_installed_mw_by_year = (
        installed_mw_by_year.data
        .set_index('year')
        .sort_index()
        .loc[:last_complete_year]
    )
    

    years_until_2030 = 2030 - last_complete_year

    annual_needed_to_meet_goal = installed_remaining['remaining'].item() / years_until_2030
    
    summary_installed_remaining = (
        installed_remaining
        .assign(
            annual_needed_to_meet_goal = annual_needed_to_meet_goal,
            as_of = end_date_of_last_complete_year
        )
    )


    # VALIDATE

    if (
        summary_installed_mw_by_year['total_installed_mw'].between(0,500).all()
        and
        installed_remaining[['installed','remaining']].iloc[0].between(0,1000).all()
    ):

        # SAVE

        data_dir = pathlib.Path('Data/Summary Data')
        data_dir.mkdir(exist_ok=True, parents=True)

        summary_installed_mw_by_year.to_csv(
            data_dir / 'solar_installed_mw_by_year.csv'
        )

        summary_installed_remaining.to_csv(
            data_dir / 'solar_installed_remaining.csv',
            index=False
        )

        return {
            'summary_installed_mw_by_year':summary_installed_mw_by_year,
            'summary_installed_remaining':summary_installed_remaining
        }

    else:
        logger.error('Incorrect data: %s', summary_installed_mw_by_year.tail(20))

        return None

if __name__ == "__main__":
    run()