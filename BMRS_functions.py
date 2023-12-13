'''
python script to call the BMRS API and return the data in a pandas dataframe
'''

# Import libraries
import requests
import pandas as pd
import pydantic
import datetime
from pathlib import Path

from secret import api_key

# define the request class
class ElexonRequest(pydantic.BaseModel):
    """Class to define the request to Elexon

    Args:
        pydantic (BaseModel): base model
    """
    report: str
    date: datetime.date
    api_key: pydantic.SecretStr = pydantic.SecretStr(api_key)
    service_type: str = 'csv'
    period: str = '*'

# function to send request to Elexon, clean save the file
def send_elexon_request(req: ElexonRequest) -> pd.DataFrame:
    """Send request to Elexon, clean and save the file

    Args:
        req (ElexonRequest): request object

    Returns:
        pandas.DataFrame: dataframe of data
    """
    # define the url
    url = f"https://api.bmreports.com/BMRS/{req.report}/v1?APIKey={req.api_key.get_secret_value()}&Period={req.period}&SettlementDate={req.date.isoformat()}&ServiceType={req.service_type}"
    # make the request
    res = requests.get(url)
    # check the status code
    assert res.status_code == 200
    
    # set the path desired to save the file
    fi = Path().cwd()/'data'/f'{req.report}_{req.date.isoformat()}.csv'
    # make the parent directory if it doesn't exist
    fi.parent.mkdir(parents=True, exist_ok=True)
    # write the reponse to the file 
    fi.write_text(res.text)

    # open the file and clean
    data = pd.read_csv(fi, skiprows=4)
    # change the column names so that they are consistent
    # some have a space in the name and some don't
    data.columns = [c.replace(' ', '') for c in data.columns]
    # drop rows if they have nan values and a valid settlement period
    data = data.dropna(axis=0, subset=['SettlementDate'])

    return data

# function to combine functions into one request
def BMRS_request(report: str, date: str) -> pd.DataFrame:
    """Get data from BMRS API

    Args:
        report (str): BMRS report name
        date (str): date in YYYY-MM-DD format

    Returns:
        pandas.DataFrame: dataframe of data
    """
    req = ElexonRequest(report=report, date=date)
    data = send_elexon_request(req)
    return data

def get_BMRS_data(report: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Get data from BMRS API

    Args:
        report (str): BMRS report name
        start_date (str): start date in YYYY-MM-DD format
        end_date (str): end date in YYYY-MM-DD format

    Returns:
        pandas.DataFrame: dataframe of data
    """
    # create a list of dates
    date_list = pd.date_range(start_date, end_date, freq="D")

    dataset = {}
    for date in date_list:
        data = BMRS_request(report, date.strftime("%Y-%m-%d"))
        dataset[date.strftime("%Y-%m-%d")] = data

    # combine all the dataframes into one
    keys = list(dataset.keys())
    df = pd.DataFrame()
    for key in keys:
        df = pd.concat([df, dataset[key]], ignore_index=True)

    return df
