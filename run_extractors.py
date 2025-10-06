import importlib
import logging

import climate_dash_tools.logging_config

logger = climate_dash_tools.logging_config.setup_logging_for_main()

def run_all():

    PIPELINES = (
        'template_pipeline',
        'organics_collection_buildings',
        'energy_star_scores',
        'diversion_rate',
        'ghg_emissions'
    )

    results = {}

    for pipeline_name in PIPELINES:

        logger.info('▶ starting %s', pipeline_name)
        pipeline = importlib.import_module('pipelines.' + pipeline_name)
        
        results[pipeline_name] = pipeline.run()


    return results


if __name__ == "__main__":
    run_all()
