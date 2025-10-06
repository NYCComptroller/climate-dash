def run():
    import pathlib

    import pandas as pd
    import geopandas as gpd

    import climate_dash_tools.extract
    import climate_dash_tools.transform
    import climate_dash_tools.logging_config

    pipeline_name = pathlib.Path(__file__).stem

    # set up logging
    logger = climate_dash_tools.logging_config.setup_logging_for_pipeline(pipeline_name)

    # EXTRACT

    table_id = '5zyy-y8am'

    # Step 1: Get the most recent year in the table

    max_year_query = '''SELECT MAX(`report_year`)'''

    max_year = climate_dash_tools.extract.from_open_data(table_id,max_year_query,parse=False)[0].get('MAX_report_year')

    logger.debug('Max year found in data: %s', max_year)

    # Step 2: Get all building scores and grades for the most recent year 
    # sort by property_id and energy_star_score (descending)

    building_grades_query = f'''
    SELECT 
    `property_id`,
    `energy_star_score` AS `ENERGY_STAR_Score`,
    CASE 
        WHEN `energy_star_score`::number >= 85 THEN 'A'
        WHEN `energy_star_score`::number >= 70 THEN 'B'
        WHEN `energy_star_score`::number >= 55 THEN 'C'
        WHEN `energy_star_score`::number >= 0 THEN 'D'
        ELSE 'na'
    END AS `Energy_Rating`,
    `address_1` AS Address,
    `city` AS City,
    `largest_property_use_type` AS `Largest_Property_Use_Type`,
    `latitude`,
    `longitude`
    WHERE (`report_year` = {max_year})
    AND(`energy_star_score` != 'Not Available')
    ORDER BY `property_id` ASC, `energy_star_score` DESC
    LIMIT 100000000
    '''

    building_grades = climate_dash_tools.extract.from_open_data(table_id,building_grades_query)

    # Step 3: Drop duplicated property_id rows, keeping first (highest)

    deduplicated_buildings_scores = (
        building_grades
        .drop_duplicates(subset='property_id', keep='first')
    )

    deduplicated_buildings_scores_geo = gpd.GeoDataFrame(
        data=deduplicated_buildings_scores,
        geometry=gpd.points_from_xy(
            deduplicated_buildings_scores['longitude'],
            deduplicated_buildings_scores['latitude'],
            crs=4326
        )
    )

    # Step 4: count instances of each grade

    count_and_proportion_by_grade = (
        pd.concat([
            (
                deduplicated_buildings_scores
                ['Energy_Rating']
                .value_counts()
            ),
            (
                deduplicated_buildings_scores
                ['Energy_Rating']
                .value_counts(normalize=True)
            )
        ],axis=1)
        .sort_index()
    )

    # VALIDATE

    if (
        count_and_proportion_by_grade['count'].ge(0).all()
        and
        count_and_proportion_by_grade['count'].max() < 50_000
    ):

        # SAVE

        data_dir = pathlib.Path('Data/Summary Data')
        data_dir.mkdir(exist_ok=True)

        deduplicated_buildings_scores_geo.to_file(
            data_dir / 'energy_star_scores__deduplicated_buildings_scores.geojson'
        )

        count_and_proportion_by_grade.to_csv(
            data_dir / 'energy_star_scores__count_and_proportion_by_grade.csv'
        )

        return {
            'deduplicated_buildings_scores_geo':deduplicated_buildings_scores_geo,
            'count_and_proportion_by_grade':count_and_proportion_by_grade
        }

    else:
        logger.error('Incorrect data: %s', count_and_proportion_by_grade.tail(20))

        return None

if __name__ == "__main__":
    run()