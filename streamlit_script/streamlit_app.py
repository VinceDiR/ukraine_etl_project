"""Streamlit app to create ACLED data dashboards"""
import os
from datetime import datetime, timedelta
import streamlit as st
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import plotly.express as px

acled_bucket = os.getenv("S3_BUCKET")
acled_db = os.getenv("DATABASE")
acled_table = os.getenv("TABLE")

aws_region = os.getenv("AWS_REGION")
aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_key = os.getenv("AWS_SECRET_KEY")

athena = connect(
    s3_staging_dir=f"s3://{acled_bucket}/tmp/",
    region_name=f"{aws_region}",
    aws_access_key_id=f"{aws_access_key}",
    aws_secret_access_key=f"{aws_secret_key}",
    cursor_class=PandasCursor,
).cursor()

st.set_page_config(
    page_title="Ukraine War Dashboard", page_icon=":flag-ua:", layout="wide"
)
st.title("Tracking the Conflict in Ukraine :flag-ua:")
st.subheader("Data last updated: " + (datetime.today() - timedelta(days=11)).strftime("%d %B, %Y"))

@st.cache
def get_daily_data(date1, date2):
    """Query Athena and return results as a Pandas dataframe"""
    athena_df = athena.execute(
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
        date(event_date) as event_date
        from {acled_db}.{acled_table}
        where event_date between '{date1}' and '{date2}'
        order by event_date"""
    ).as_pandas()
    athena_df["event_date"] = athena_df["event_date"].dt.strftime("%Y-%m-%d")
    athena_df["upload_date"] = athena_df["upload_date"].dt.strftime("%Y-%m-%d")
    return athena_df


st.sidebar.title("Select Date Range")

with st.sidebar:
    date_choice = st.date_input(
        "Choose Start Date",
        (
            datetime(2022, 2, 24),
            (datetime.today() - timedelta(days=11)),
        ),
        datetime(2022, 2, 24),
        (datetime.today() - timedelta(days=11)),
    )

    gen_dash = st.button("Generate Data and Map")


if gen_dash:
    df = get_daily_data(
        datetime.strftime(date_choice[0], "%Y-%m-%d"),
        datetime.strftime(date_choice[1], "%Y-%m-%d"),
    )

    with st.expander("Show Raw DataFrame"):
        st.write(df)
    fig1 = px.scatter_mapbox(
        df,
        lat="latitude",
        lon="longitude",
        hover_name="data_id",
        hover_data={
            "actor1": True,
            "actor2": True,
            "event_date": True,
            "event_type": True,
            "notes": True,
            "source": True,
            "fatalities": True,
            "latitude": False,
            "longitude": False,
        },
        color="event_type",
        size=(df["fatalities"] + 10).to_list(),
        mapbox_style="carto-positron",
        zoom=6,
        height=1000,
    )
    fig1.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    fig1.update_traces(
        hovertemplate="<b>%{hovertext}</b><br><br>Actor 1: %{customdata[0]}<br>Actor 2: %{customdata[1]}<br>Event Date: %{customdata[2]}<br>Event Type: %{customdata[3]}<br>Notes: %{customdata[4]}<br>Source: %{customdata[5]}<br>Fatalities: %{customdata[6]}"
    )
    st.plotly_chart(fig1, use_container_width=True)

    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.metric("Total Days", len(df["event_date"].unique()))
    with col2:
        st.metric("Total Events", len(df["data_id"].unique()))
    with col3:
        st.metric("Total Reported Fatalities", df["fatalities"].sum())
    with col4:
        st.metric(
            "Total Explosions/Remote violence",
            len(df[df["event_type"] == "Explosions/Remote violence"]),
        )
    with col5:
        st.metric("Total Battles", len(df[df["event_type"] == "Battles"]))
    with col6:
        st.metric(
            "Total Instances of Violence Against Civilians",
            len(df[df["event_type"] == "Violence against civilians"]),
        )

    with st.expander("Show Timelapse Visualization"):
        fig2 = px.scatter_mapbox(
            df,
            lat="latitude",
            lon="longitude",
            hover_name="data_id",
            hover_data={
                "actor1": True,
                "actor2": True,
                "event_date": True,
                "event_type": True,
                "notes": True,
                "source": True,
                "fatalities": True,
                "latitude": False,
                "longitude": False,
            },
            color="event_type",
            size=(df["fatalities"] + 10).to_list(),
            animation_frame="event_date",
            mapbox_style="carto-positron",
            zoom=6,
            height=1000,
        )
        fig2.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        fig2.update_traces(
            hovertemplate="<b>%{hovertext}</b><br><br>Actor 1: %{customdata[0]}<br>Actor 2: %{customdata[1]}<br>Event Date: %{customdata[2]}<br>Event Type: %{customdata[3]}<br>Notes: %{customdata[4]}<br>Source: %{customdata[5]}<br>Fatalities: %{customdata[6]}"
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.plotly_chart(
        px.bar(
            df,
            x="event_date",
            y="fatalities",
            color="event_type",
            height=1000,
            title="Daily Reported Fatalities by Event Type",
        ),
        use_container_width=True,
    )

    fig3 = px.bar(
        df,
        x=df.event_date.unique(),
        y=[
            df[df.actor1.str.contains("Russia")]
            .groupby("event_date")["data_id"]
            .count(),
            df[df.actor1.str.contains("Ukraine")]
            .groupby("event_date")["data_id"]
            .count(),
        ],
        barmode="group",
        hover_name=df.event_date.unique(),
        height=500,
        title="Number of Events Initiated by Russian and Ukrainian Forces per Date",
    )
    newnames = {"wide_variable_0": "Russia", "wide_variable_1": "Ukraine"}
    fig3.for_each_trace(
        lambda t: t.update(
            name=newnames[t.name],
            legendgroup=newnames[t.name],
            hovertemplate=t.hovertemplate.replace(t.name, newnames[t.name]),
        )
    )
    st.plotly_chart(fig3, use_container_width=True)
