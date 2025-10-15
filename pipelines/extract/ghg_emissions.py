def run():
    import pathlib
    import re

    # import pandas as pd

    import climate_dash_tools.extract
    # import climate_dash_tools.transform
    import climate_dash_tools.logging_config

    pipeline_name = pathlib.Path(__file__).stem

    # set up logging
    logger = climate_dash_tools.logging_config.setup_logging_for_pipeline(pipeline_name)

    # EXTRACT

    table_id = 'wq7q-htne'

    # Step 1: Find the most recent year of data available

    column_names_query = '''
    SELECT *
    LIMIT 1
    '''

    one_row = climate_dash_tools.extract.from_open_data(
        table_id=table_id,
        query=column_names_query
    )

    # Filter to '*_tco2e' columns 
    columns_pattern = r'(^cy_\d{4}_tco2e(_100_yr_gwp|$))'

    tco2e_cols = one_row.columns.str.extract(columns_pattern)[0].dropna().to_list()

    # Find max (i.e. highest year) of those columns
    max_tco2e_col_name = max(tco2e_cols)

    # Extract year from that column name
    year = re.search(pattern=r'20\d{2}',string=max_tco2e_col_name)
    max_tco2e_col_year = int(year.group()) if year else None 

    logger.debug('most recent year of tco2e data: %s', max_tco2e_col_year)

    # Step 2: Query data for most recent year

    # Total by sector
    query = f'''
    SELECT
        `sectors_sector` AS `sector`,
        {','.join([f'SUM({col})' for col in tco2e_cols])}
    WHERE `sectors_sector` != 'Total'
    GROUP BY `sectors_sector`
    '''
    total_by_sector = climate_dash_tools.extract.from_open_data(table_id,query)

    total_by_sector = total_by_sector.set_index('sector')

    # Extract year from column names and rename columns to just the year
    pattern = r'(20\d{2})'
    total_by_sector.columns = total_by_sector.columns.str.extract(pattern)[0].astype(int)

    # Reshape to tidy
    total_by_sector = (
        total_by_sector
        .rename_axis(columns='year')
        .T
        .melt(ignore_index=False, value_name='total')
        .set_index('sector',append=True)
        .sort_index()
    )

    # Buildings

    buildings_by_sector_by_fuel_query = f'''
    SELECT
        `category_label`,
        CASE 
            WHEN `source_label` IN ('#2 fuel oil', '#4 fuel oil', '#6 fuel oil') THEN 'Fuel oil'
            ELSE `source_label`
        END AS `source_group`,
        SUM({max_tco2e_col_name}) AS total
    WHERE
        sectors_sector = 'Stationary Energy'
        AND `inventory_type` = 'GPC'
        AND `category_label` != 'Fugitive'
        AND `source_label` != 'Biofuel'
    GROUP BY `category_label`, `source_group`
    '''

    buildings_by_sector_by_fuel = climate_dash_tools.extract.from_open_data(table_id,buildings_by_sector_by_fuel_query)

    # Reshape to tidy
    buildings_by_sector_by_fuel = (
        buildings_by_sector_by_fuel
        .set_index(['category_label','source_group'])
        .sort_index()
    )

    buildings_change_query = f'''
    SELECT
        `category_label`,
        `source_label`,
        SUM(`cy_2005_tco2e_100_yr_gwp`) AS `total_2005`,
        SUM(`{max_tco2e_col_name}`) AS `total_{max_tco2e_col_year}`,
        (SUM(`{max_tco2e_col_name}`) - SUM(`cy_2005_tco2e_100_yr_gwp`)) / SUM(`cy_2005_tco2e_100_yr_gwp`) AS `pct_change`
    WHERE
        (`sectors_sector` == "Stationary Energy")
        AND (`inventory_type` == "GPC")
        AND (`category_label` != "Fugitive")
        AND (`source_label` != "Biofuel")
    GROUP BY `category_label`, `source_label`
    '''
    buildings_change = climate_dash_tools.extract.from_open_data(table_id,buildings_change_query)

    buildings_change = (
        buildings_change
        .set_index(['category_label','source_label'])
        .sort_index()
    )

    # Transportation

    transportation_change_query = f'''
    SELECT
        `category_label`,
        SUM(`cy_2005_tco2e_100_yr_gwp`) AS `total_2005`,
        SUM(`{max_tco2e_col_name}`) AS `total_{max_tco2e_col_year}`,
        (SUM(`{max_tco2e_col_name}`) - SUM(`cy_2005_tco2e_100_yr_gwp`)) / SUM(`cy_2005_tco2e_100_yr_gwp`) AS `pct_change`
    WHERE
        (`sectors_sector` == "Transportation")
    GROUP BY `category_label`
    '''

    transportation_change = climate_dash_tools.extract.from_open_data(table_id,transportation_change_query)

    transportation_change = transportation_change.set_index('category_label')

    # VALIDATE

    if (
        total_by_sector.loc[(slice(None),'Transportation'),'total'].between(5_000_000, 25_000_000).all()
        and
        total_by_sector.loc[(slice(None),'Stationary Energy'),'total'].between(20_000_000,80_000_000).all()
        and
        total_by_sector.loc[(slice(None),'Waste'),'total'].between(500_000,4_000_000).all()
        and
        buildings_by_sector_by_fuel.ge(10_000).all().all()
        and
        buildings_change['pct_change'].between(-3,3).all()
        and
        transportation_change['pct_change'].between(-3,3).all()
    ):

        # SAVE

        data_dir = pathlib.Path('Data/Summary Data')
        data_dir.mkdir(exist_ok=True, parents=True)

        total_by_sector.to_csv(
            data_dir / 'ghg_emissions__total_by_sector.csv'
        )

        buildings_by_sector_by_fuel.to_csv(
            data_dir / 'ghg_emissions__buildings_by_sector_by_fuel.csv'
        )

        buildings_change.to_csv(
            data_dir / 'ghg_emissions__buildings_change.csv'
        )

        transportation_change.to_csv(
            data_dir / 'ghg_emissions__transportation_change.csv'
        )

        return {
            'total_by_sector':total_by_sector,
            'buildings_by_sector_by_fuel':buildings_by_sector_by_fuel,
            'buildings_change':buildings_change,
            'transportation_change':transportation_change
        }

    else:
        logger.error(
            'Incorrect data: %s | %s | %s | %s',
            total_by_sector,
            buildings_by_sector_by_fuel,
            buildings_change,
            transportation_change
        )

        return None

if __name__ == "__main__":
    run()