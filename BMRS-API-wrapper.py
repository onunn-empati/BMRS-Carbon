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
    report: str
    date: datetime.date
    api_key: pydantic.SecretStr = pydantic.SecretStr(api_key)
    service_type: str = 'csv'
    period: str = '*'

# function to send request to Elexon, clean save the file
def send_elexon_request(req: ElexonRequest) -> pd.DataFrame:
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
