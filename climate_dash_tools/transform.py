import pandas as pd

# def get_last_complete_fy(metadata):
#     """Get most recent full fiscal year preceding the data-last-updated timestamp.
#     Returns pd.Timestamp
#     """
#     last_updated = pd.to_datetime(metadata['dataUpdatedAt'])
    
#     if last_updated.tz:
#         last_updated = last_updated.tz_convert('US/Eastern')

#     return (
#         last_updated
#         .tz_localize(None)
#         - pd.tseries.offsets.YearEnd(month=6,normalize=True)
#     )

def get_last_complete_period_end_date(metadata,freq):
    """Get end date of the most recent full period preceding the data-last-updated timestamp.

    Parameters
    ----------
    metadata : dict
        metadata dict returned from OpenData by ``extracts.from_open_data`` including a `dataUpdatedAt`

    freq : str
        pandas freq str / offset alias
        Use `'YE'` for end of preceding calendar year
        Use `'YE-JUN'` for end of preceding fiscal year

    Returns pd.Timestamp
    """
    last_updated = pd.to_datetime(metadata['dataUpdatedAt'])
    
    if last_updated.tz:
        last_updated = last_updated.tz_convert('US/Eastern')

    return (
        last_updated
        .tz_localize(None)
        - pd.tseries.frequencies.to_offset(freq)
    ).normalize()



