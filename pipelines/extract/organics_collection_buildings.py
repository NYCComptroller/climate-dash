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

    table_id = 'tiyn-ajjm'

    query = '''
    SELECT
        `fiscal_year`,
        `number_of_1_9_unit_buildings`,
        `number_of_10_unit_buildings`,
        `total_number_of_schools_receiving_curbside_organics_collection`
    '''

    organics_collection_buildings = climate_dash_tools.extract.from_open_data(
        table_id=table_id,
        query=query,
    )

    # TRANSFORM

    summary_data = (
        organics_collection_buildings
        .melt(
            id_vars=['fiscal_year'],
            var_name='measure',
            value_name='count'
        )
        .set_index(['fiscal_year','measure'])

    )

    # VALIDATE

    if (
        summary_data['count'].ge(0).all()
        and
        summary_data.loc[:,'total_number_of_schools_receiving_curbside_organics_collection',:].max().lt(10_000).all()
        and
        summary_data.loc[:,'number_of_1_9_unit_buildings',:].max().lt(5_000_000).all()
    ):

        # SAVE

        data_dir = pathlib.Path('Data/Summary Data')
        data_dir.mkdir(exist_ok=True, parents=True)

        summary_data.to_csv(
            data_dir / f'{pipeline_name}.csv',
        )

        return summary_data

    else:
        logger.error('Incorrect data: %s', summary_data.tail(20))

        return None

if __name__ == "__main__":
    run()