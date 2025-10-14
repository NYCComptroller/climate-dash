import json
import logging
from collections import namedtuple
import os
from typing import Union, Tuple, List, Dict, Any, Literal, NamedTuple
import requests
import pandas as pd

# from climate_dash.config.settings import settings

# Type aliases
OpenDataCollection = Literal['city', 'state']
RawData = List[Dict[str, Any]]
Metadata = Dict[str, Any]

class Dataset(NamedTuple):
    data:pd.DataFrame
    metadata:Metadata

logger = logging.getLogger(__name__)

def _load_token() -> str:
    from dotenv import load_dotenv
    load_dotenv()

    token = os.getenv('OPEN_DATA_APP_TOKEN')
    
    if not token:
        raise ValueError("OPEN_DATA_APP_TOKEN not set. Put it in .env or env vars.")
    return token


def _construct_open_data_urls(
    table_id: str, 
    open_data_collection: OpenDataCollection = 'city'
) -> Dict[str, str]:
    """
    Creates urls for metadata and data requests.
    """

    BASE_URLS = {
        'city':'https://data.cityofnewyork.us/',
        'state':'https://data.ny.gov/'
    }

    base_url = BASE_URLS[open_data_collection]

    data_request_url = (
        base_url
        + 'resource/'
        + table_id
        + '.json'
    )

    metadata_request_url = (
        base_url
        + 'api/views/metadata/v1/'
        + table_id
    )

    return {
        'data_request_url': data_request_url,
        'metadata_request_url': metadata_request_url
    }


def _request_data(
    table_id: str,
    open_data_collection: OpenDataCollection = 'city',
    query: str = 'SELECT * LIMIT 1000000'
) -> Tuple[RawData, Dict[str, str]]:
    request_urls = _construct_open_data_urls(
        table_id=table_id,
        open_data_collection=open_data_collection
    )

    token = _load_token()

    params = {
        '$query': query
    }

    headers = {
        'X-App-Token': token
    }

    r = requests.get(
        request_urls.get('data_request_url'),
        headers=headers,
        params=params,
        timeout=300
    )

    try:
        r.raise_for_status()

        data_json = r.json()

        if isinstance(data_json, list) and len(data_json) in (1000,1000000):
            logger.warning('Data was truncated at %s rows. Increase LIMIT in query to get full data.', len(data_json))
        else:
            logger.info("Received %s rows", len(data_json) if isinstance(data_json, list) else 'unknown')

        return data_json, r.headers

    except requests.HTTPError as e:
        try:
            error_response = r.json()
        except json.JSONDecodeError as e:
            error_response = None
        logger.error('connection error. status code: %s, response: %s', r.status_code, error_response)
        raise


def _parse_data(
    data_json: RawData, 
    response_headers: Dict[str, str]
) -> pd.DataFrame:
    def convert_column(col, dtype):
        if dtype in ('floating_timestamp', 'fixed_timestamp'):
            return pd.to_datetime(col, errors='coerce', utc=True)
        elif dtype == 'number':
            return pd.to_numeric(col, errors='coerce')
        else:
            return col

    if response_headers is None:
        logger.warning('No data types returned in response. Not converting types')
        return pd.DataFrame(data_json)

    fields_raw = response_headers.get('X-SODA2-Fields')
    types_raw = response_headers.get('X-Soda2-Types')

    if not fields_raw or not types_raw:
        logger.warning('No data types returned in response. Not converting types')
        return pd.DataFrame(data_json)

    fields = json.loads(fields_raw)
    dtypes = json.loads(types_raw)

    dtype_dict = dict(zip(fields, dtypes))

    df = pd.DataFrame(data_json)

    if df.empty:
        logger.warning('No data.')
        return df
    else:
        df = df.apply(
            lambda col: convert_column(col, dtype_dict.get(col.name, None))
        )

    return df


def _request_metadata(
    table_id: str, 
    open_data_collection: OpenDataCollection = 'city'
) -> Metadata:
    request_urls = _construct_open_data_urls(
        table_id=table_id,
        open_data_collection=open_data_collection
    )
    metadata_request_url = request_urls.get('metadata_request_url')
    r = requests.get(metadata_request_url, timeout=500)

    try:
        r.raise_for_status()
        return r.json()

    except requests.HTTPError as e:
        try:
            error_response = r.json()
        except json.JSONDecodeError as e:
            error_response = None
        logger.error('connection error. status code: %s, response: %s', r.status_code, error_response)
        raise


# public API

def from_open_data(
    table_id: str,
    query: str = 'SELECT * LIMIT 1000000',
    open_data_collection: OpenDataCollection = 'city',
    parse: bool = True,
    include_metadata: bool = False
) -> Union[pd.DataFrame, RawData, Tuple[Union[pd.DataFrame, RawData], Metadata]]:
    """
    Fetch data (and optionally metadata) from NYC Open Data or NYS Open Data.

    Parameters
    ----------

    table_id : str
        NYC OpenData table_id, e.g. `5e9h-x6ak`

    query : str, optional
        SQL query to pass to OpenData. 
        This SQL flavor accepts date manipulations such as [`date_extract_y`](https://dev.socrata.com/docs/datatypes/floating_timestamp#,) and geographic filters such as [`within_box`](https://dev.socrata.com/docs/functions/within_box) 

    open_data_collection : {'city','state'}, default 'city'
        OpenData library to extract from. use 'city' for NYC OpenData or 'state' for NYS data.ny.gov

    parse : bool, default True
        if True, parse data types as specified from source table and return DataFrame. Else, return raw json list
    
    include_metadata : bool, default False
        also return table metadata.

    Returns
    -------
    Union[pd.DataFrame, RawData, Tuple[Union[pd.DataFrame, RawData], Metadata]]
        If include_metadata is False:
            - pd.DataFrame if parse=True
            - raw list JSON if parse=False
        If include_metadata is True:
            - Dataset named tuple with (`data`, `metadata`)
    """
    data_json, response_headers = _request_data(
        table_id=table_id,
        open_data_collection=open_data_collection,
        query=query
    )

    if parse:
        data = _parse_data(data_json, response_headers)
    else:
        data = data_json

    if include_metadata:
        metadata = _request_metadata(table_id, open_data_collection)
        return Dataset(data, metadata)
    
    return data