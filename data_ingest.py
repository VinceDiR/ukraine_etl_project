"""Module docstring"""
import requests
from dotenv import load_dotenv
import pandas as pd
import json
import os
from datetime import datetime

load_dotenv()

api_key = os.getenv("ACLED_ACCESS_KEY")
username = os.getenv("ACLED_USERNAME")
today = datetime.today().strftime("%Y-%m-%d")


def pull_data():
    df = pd.read_csv("./data/ukraine.csv")
    df.drop(columns=["Unnamed: 0"], inplace=True)
    df = df.astype(
        {
            "data_id": "int",
            "iso": "int",
            "event_id_cnty": "str",
            "event_id_no_cnty": "int",
            "event_date": "datetime64[ms]",
            "year": "int",
            "time_precision": "int",
            "event_type": "str",
            "actor1": "str",
            "assoc_actor_1": "str",
            "inter1": "int",
            "actor2": "str",
            "assoc_actor_2": "str",
            "inter2": "int",
            "interaction": "int",
            "region": "str",
            "country": "str",
            "admin1": "str",
            "admin2": "str",
            "admin3": "str",
            "location": "str",
            "latitude": "float",
            "longitude": "float",
            "geo_precision": "int",
            "source": "str",
            "source_scale": "str",
            "notes": "str",
            "fatalities": "int",
            "timestamp": "datetime64[ms]",
        }
    )
    df["event_date"] = df["event_date"].dt.strftime("%Y-%m-%d")
    start_date = df["event_date"].max()
    end_date = datetime.today().strftime("%Y-%m-%d")
    date_list = (
        pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    )
    for date in date_list:
        response = requests.get(
            f"https://api.acleddata.com/acled/read?key={api_key}&email={username}&event_date={date}&iso=804"
        )
        data = response.json()
        df2 = pd.read_json(json.dumps(data["data"]))
        if len(df) > 0:
            df2 = df2.astype(
                {
                    "data_id": "int",
                    "iso": "int",
                    "event_id_cnty": "str",
                    "event_id_no_cnty": "int",
                    "event_date": "datetime64[ms]",
                    "year": "int",
                    "time_precision": "int",
                    "event_type": "str",
                    "actor1": "str",
                    "assoc_actor_1": "str",
                    "inter1": "int",
                    "actor2": "str",
                    "assoc_actor_2": "str",
                    "inter2": "int",
                    "interaction": "int",
                    "region": "str",
                    "country": "str",
                    "admin1": "str",
                    "admin2": "str",
                    "admin3": "str",
                    "location": "str",
                    "latitude": "float",
                    "longitude": "float",
                    "geo_precision": "int",
                    "source": "str",
                    "source_scale": "str",
                    "notes": "str",
                    "fatalities": "int",
                    "timestamp": "datetime64[ms]",
                }
            )
            df2["event_date"] = df2["event_date"].dt.strftime("%Y-%m-%d")
            df = pd.concat([df, df2], axis=0, ignore_index=True)
            df_unq = df.groupby(df.columns.tolist(), as_index=False).size()
            assert df_unq["size"].max() == 1
            return df


df = pull_data()
df
