"""Streamlit app to create ACLED data dashboards"""
import os
from datetime import datetime, timedelta
from time import strftime
from pandas import date_range
import streamlit as st
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

st.set_page_config(page_title="Ukraine War Dashboard", page_icon=":flag-ua:", layout="wide")

acled_bucket = os.getenv("S3_BUCKET")
acled_db = os.getenv("DATABASE")
acled_table = os.getenv("TABLE")

aws_region = os.getenv("AWS_REGION")
aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_key = os.getenv("AWS_SECRET_KEY")

st.title(":flag-ua: Ukraine War Dashboard")

col = st.columns(1)

@st.cache
def get_daily_data(date):
    """Query Athena and return results as a Pandas dataframe"""
    athena = connect(
        s3_staging_dir=f"s3://{acled_bucket}/tmp/",
        region_name=f"{aws_region}",
        aws_access_key_id=f"{aws_access_key}",
        aws_secret_access_key=f"{aws_secret_key}",
        cursor_class=PandasCursor,
    ).cursor()
    return athena.execute(
        f"""SELECT * FROM {acled_db}.{acled_table} WHERE event_date LIKE '{date}'"""
    ).as_pandas()

st.sidebar.title("Filter by date")

with st.sidebar:
        date_choice = st.selectbox(
        "Choose Date",
        [
            strftime("%Y-%m-%d", d.timetuple())
            for d in date_range(
                start="2022-02-24",
                end=strftime(
                    "%Y-%m-%d", (datetime.today() - timedelta(days=8)).timetuple()
                ),
            )
        ],
    )
        with st.sidebar:
            gen_dash = st.button("Generate Dashboard")

with col[0]:
    if gen_dash:
        df = get_daily_data(date_choice)
        with st.expander("Show Raw DataFrame"):
            st.write(df)
        st.map(data=df[["latitude", "longitude"]], zoom=5)
