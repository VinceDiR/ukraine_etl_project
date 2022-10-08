"""Streamlit app to create ACLED data dashboards"""
import os
from datetime import datetime, timedelta
import streamlit as st
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import altair as alt

st.set_page_config(
    page_title="Ukraine War Dashboard", page_icon=":flag-ua:", layout="wide"
)

acled_bucket = os.getenv("S3_BUCKET")
acled_db = os.getenv("DATABASE")
acled_table = os.getenv("TABLE")

aws_region = os.getenv("AWS_REGION")
aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_key = os.getenv("AWS_SECRET_KEY")

st.title(":flag-ua: Tracking the Conflict in Ukraine")

col = st.columns(1)


@st.cache
def get_daily_data(date1, date2):
    """Query Athena and return results as a Pandas dataframe"""
    athena = connect(
        s3_staging_dir=f"s3://{acled_bucket}/tmp/",
        region_name=f"{aws_region}",
        aws_access_key_id=f"{aws_access_key}",
        aws_secret_access_key=f"{aws_secret_key}",
        cursor_class=PandasCursor,
    ).cursor()
    return athena.execute(
        f"""select
        "actor1",
        "actor2",
        "admin1",
        "admin2",
        "admin3",
        "assoc_actor_1",
        "assoc_actor_2",
        "country",
        "data_id",
        "event_id_cnty",
        "event_id_no_cnty",
        "event_type", "fatalities",
        "geo_precision",
        "inter1",
        "inter2",
        "interaction",
        "iso", "iso3",
        "latitude",
        "location",
        "longitude",
        "notes",
        "region",
        "source",
        "source_scale",
        "sub_event_type",
        "time_precision",
        "upload_date",
        "year",
        from_iso8601_date(event_date) as event_date
        from {acled_db}.{acled_table}
        where event_date between '{date1}' and '{date2}'
        order by event_date"""
    ).as_pandas()


st.sidebar.title("Select Date Range")

with st.sidebar:
    date_choice = st.date_input(
        "Choose Start Date",
        (
            datetime(2022, 2, 24),
            (datetime.today() - timedelta(days=8)),
        ),
        datetime(2022, 2, 24),
        (datetime.today() - timedelta(days=8)),
    )
    with st.sidebar:
        gen_dash = st.button("Generate Dashboard")

with col[0]:
    if gen_dash:
        df = get_daily_data(
            datetime.strftime(date_choice[0], "%Y-%m-%d"),
            datetime.strftime(date_choice[1], "%Y-%m-%d"),
        )
        df["event_date"] = df["event_date"].astype("datetime64[D]")
        with st.expander("Show Raw DataFrame"):
            st.dataframe(df)
        c = (
            alt.Chart(df)
            .mark_circle()
            .encode(
                x="latitude",
                y="longitude",
                size="fatalities",
                color="event_type",
                tooltip=[
                    "notes",
                    "event_date",
                    "fatalities",
                    "event_type",
                    "actor1",
                    "actor2",
                ],
            )
        )
        st.altair_chart(c, use_container_width=True)
