# These are not on OpenData. Extract from data on GitHub

import pathlib

import pandas as pd
import requests

import climate_dash_tools.logging_config

def run():
    
    pipeline_name = pathlib.Path(__file__).stem

    # set up logging
    logger = climate_dash_tools.logging_config.setup_logging_for_pipeline(pipeline_name)

    def get_newest_data_for_cd(
        indicator_id,
        measure_id,
        time_period_table,
        measures_metadata_table
    ):

        # # Fetch time period and measure metadata tables 
        # if time_period_table is None:
        #     time_period_table = pd.read_json('https://raw.githubusercontent.com/nychealth/EHDP-data/refs/heads/production/indicators/metadata/TimePeriods.json')

        # if measures_metadata_table is None:
        #     r = requests.get('https://raw.githubusercontent.com/nychealth/EHDP-data/refs/heads/production/indicators/metadata/metadata.json')
        #     r.raise_for_status()
        #     metadata_json = r.json()
        #     measures_metadata_table = pd.json_normalize(metadata_json,record_path='Measures')

        # Fetch data table for this indicator
        data_table_url = f"https://raw.githubusercontent.com/nychealth/EHDP-data/refs/heads/production/indicators/data/{indicator_id}.json"
        data_table = pd.read_json(data_table_url)

        # Find time periods available for this measure by CD
        available_time_periods_for_data_for_cd = (
            data_table
            [
                data_table['GeoType'].eq('CD')
                &
                data_table['MeasureID'].eq(measure_id)
            ]
            ['TimePeriodID']
            .unique()
        )

        # Find most recent time period
        most_recent_time_period_for_data_for_cd = (
            time_period_table
            [
                time_period_table['TimeType'].eq('year')
                &
                time_period_table['TimePeriodID'].isin(available_time_periods_for_data_for_cd)
            ]
            .sort_values('TimePeriod')
            .tail(1)
        )

        most_recent_time_period_for_data_for_cd_label = most_recent_time_period_for_data_for_cd['TimePeriod'].item()
        most_recent_time_period_for_data_for_cd_id = most_recent_time_period_for_data_for_cd['TimePeriodID'].item()

        # Filter and format data
        data = (
            data_table
            [
                
                data_table['GeoType'].eq('CD')
                &
                data_table['MeasureID'].eq(measure_id)
                &
                data_table['TimePeriodID'].eq(most_recent_time_period_for_data_for_cd_id)
            ]
            .merge(
                measures_metadata_table,
                on='MeasureID'
            )
            .assign(
                Year = most_recent_time_period_for_data_for_cd_label
            )
            .rename(columns={'DisplayType':'Unit'})
            [[
                'GeoID',
                'Year',
                'Value',
                'Unit',
                'MeasureName',
            ]]
        )

        return data


    # Get data for all indicators

    INDICATOR_MEASURE_IDS = {
        'PM25':{'indicator_id':2023,'measure_id':1425},
        'BC':{'indicator_id':2024,'measure_id':1428},
        'NO':{'indicator_id':2028,'measure_id':1436},
        'NO2':{'indicator_id':2025,'measure_id':1431},
        'O3':{'indicator_id':2027,'measure_id':1435},
    }

    time_period_table = pd.read_json('https://raw.githubusercontent.com/nychealth/EHDP-data/refs/heads/production/indicators/metadata/TimePeriods.json')

    r = requests.get('https://raw.githubusercontent.com/nychealth/EHDP-data/refs/heads/production/indicators/metadata/metadata.json')
    r.raise_for_status()
    metadata_json = r.json()
    measures_metadata_table = pd.json_normalize(metadata_json,record_path='Measures')


    air_pollution_measures = {}

    for pollutant,ids in INDICATOR_MEASURE_IDS.items():

        logger.info('getting %s', pollutant)

        data = get_newest_data_for_cd(
            **ids,
            time_period_table=time_period_table,
            measures_metadata_table=measures_metadata_table
        )

        # VALIDATE

        if (
            data['Value'].between(0,100).all()
        ):

            logger.info('%s data: ok!', pollutant)

            # SAVE

            data_dir = pathlib.Path('Data/Summary Data')
            data_dir.mkdir(exist_ok=True)

            data.to_csv(
                data_dir / f'{pollutant}_by_CD.csv'
            )

            air_pollution_measures[pollutant] = data

        else:
            logger.error('Incorrect data for %s: %s', pollutant, data.tail(20))

            air_pollution_measures[pollutant] = None

    return air_pollution_measures

if __name__ == '__main__':
    run()