import importlib

import climate_dash_tools.logging_config

logger = climate_dash_tools.logging_config.setup_logging_for_main()

def run_all():

    PIPELINES = (
        'organics_collection_buildings',
        'energy_star_scores',
        'diversion_rate',
        'ghg_emissions',
        'bicycle_lane_miles',
        'bike_parking_spaces',
        'electric_vehicles_registered',
        'ev_fleet_count',
        'installed_solar',
        'air_quality'
    )

    results = {}

    for pipeline_name in PIPELINES:

        logger.info('▶ starting %s', pipeline_name)
        pipeline = importlib.import_module('pipelines.extract.' + pipeline_name)
        
        results[pipeline_name] = pipeline.run()


    return results


if __name__ == "__main__":
    run_all()
