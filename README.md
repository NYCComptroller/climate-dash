# Get data for the NYC Climate Dashboard

This repo extracts and transforms data used for the [NYC Climate Dashboard](https://comptroller.nyc.gov/services/for-the-public/nyc-climate-dashboard/overview/). 

Data pipelines are run automatically on a schedule to get new data and save the most up-to-data summary data to the [Summary Data]("Data/Summary Data") folder.

### Data available

Summary data are available in the [Summary Data]("Data/Summary Data") folder. 

### Sources

All data included here are from public, open sources.

\[table tk]

## Repo structure

`run_extractors.py` is the main entrypoint. Calling this script or running its `run_all()` function will run all extract-transform pipelines, save summary data, and return all the summary data. 

The `pipelines` directory contains individual processes, each of which extracts data from a single source. In general, each pipeline runs a SQL query against an OpenData, normalizes or reshapes the result as needed, saves the summary to a file and also returns the summary data as a DataFrame. For data sources from which multiple summaries are needed (such as greenhouse gas inventory), the pipeline will run multiple queries and produce multiple outputs.

The `climate_dash_tools` module includes functions used for set up and querying.

### Automation

The data extract pipelines are run automatically on GitHub Actions once a month. The [.github/actions/action.yml] sets up automation. 

### Secrets

NYC OpenData requires an app token to process large queries. For GitHub Actions automation, an this repo has an app token is stored in its repo secrets. To create a copy, add a token following step 3 below. 

### Dependencies

The (few) dependencies are specified in the `pyproject.toml`. This project uses `uv` to manage dependencies. To use `uv`:
1. [install `uv`](https://docs.astral.sh/uv/getting-started/installation/)
2. use `uv run` to run modules and scripts, e.g. `uv run python -m run_extractors`. `uv` will automatically build a virtual environment with the necessary dependencies and run the program in this environment.

## How to set up your own local copy

#### 1. Clone this repo
#### 2. Install dependencies

- Use `uv` to automatically install dependencies.
	1. [install `uv`](https://docs.astral.sh/uv/getting-started/installation/)
	2. when you use `uv run` to run modules (see step 4), `uv` will automatically build a virtual environment with the necessary dependencies and run the program in this environment.
- (Alternatively, use another package manager to install the requirements in `pyproject.toml`)
  
#### 3. Add your open data app token
1. [Create an account](https://data.cityofnewyork.us/login) on NYC OpenData
2. Create an app token
   1. navigate your user name --> Developer Settings --> Create New App Token
3. Copy [`.env.template`](.env.template) as `.env`
4. Add your app token to `.env`
   
#### 4. Run
- Run all extractors with `uv run python -m run_extractors`
- Run a single pipeline with `uv run python -m pipelines.ghg_emissions` (or swap any other pipeline name from `pipelines` for `ghg emissions` here.) 

## Contributing

These extract-transforms steps have been made public so that anyone can help maintain them or add to them as source data tables, APIs, etc. may change. If data summaries are not running successfully (or if you want to extend or add to the summaries here), please contribute with a pull request! (Create a local clone, identify and fix the problem, or add the new feature, then open a pull request with the fix.)

## Reusing
Use this repo as a model or template for other automated data summarization tasks!

## Extending
A private copy of this repo includes additional steps to load the data so they can be displayed on the [NYC Climate Dashboard](https://comptroller.nyc.gov/services/for-the-public/nyc-climate-dashboard/overview/)
