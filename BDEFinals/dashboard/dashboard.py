import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
import json
import threading
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from streamlit_autorefresh import st_autorefresh
import base64
from io import BytesIO

st.set_page_config(page_title="World Bank Live Indicators", layout="wide")

# --- Configuration ---
KAFKA_BROKER = 'host.docker.internal:9092'
TOPIC_NAME = 'worldbank-data'
MONGO_URI = 'mongodb://localhost:27017'
DB_NAME = 'worldbank_db'
COLLECTION_RAW = 'indicator_history'

# --- MongoDB ---
@st.cache_resource
def init_mongo():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]

db = init_mongo()
raw_col = db[COLLECTION_RAW]

@st.cache_data(ttl=30)
def get_historical(hours=24):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    cursor = raw_col.find({"timestamp": {"$gte": cutoff.isoformat()}}).sort("timestamp", -1)
    df = pd.DataFrame(list(cursor))
    if not df.empty:
        df = df.drop(columns=['_id'], errors='ignore')
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # Remove timezone info if present (future-proof check)
        if hasattr(df['timestamp'].dtype, 'tz') and df['timestamp'].dtype.tz is not None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(None)
    return df

@st.cache_data(ttl=30)
def get_latest_from_mongo(limit=200):
    """Fallback: fetch most recent records directly from MongoDB."""
    cursor = raw_col.find().sort("timestamp", -1).limit(limit)
    df = pd.DataFrame(list(cursor))
    if not df.empty:
        df = df.drop(columns=['_id'], errors='ignore')
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # Remove timezone info if present (future-proof check)
        if hasattr(df['timestamp'].dtype, 'tz') and df['timestamp'].dtype.tz is not None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(None)
    return df

def get_available_countries():
    return raw_col.distinct('country_name')

def get_available_indicators():
    return raw_col.distinct('indicator_name')

# --- Live Consumer Thread with Retry ---
class LiveIndicatorConsumer:
    def __init__(self):
        self.messages = []
        self.running = True
        self.lock = threading.Lock()
        self.connected = False
        self.thread = threading.Thread(target=self._consume, daemon=True)
        self.thread.start()

    def _connect_kafka(self):
        """Try to connect to Kafka with retries."""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                consumer = KafkaConsumer(
                    TOPIC_NAME,
                    bootstrap_servers=KAFKA_BROKER,
                    auto_offset_reset='earliest',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    consumer_timeout_ms=10000
                )
                print(f"Live consumer connected to Kafka at {KAFKA_BROKER}")
                return consumer
            except NoBrokersAvailable:
                print(f"Attempt {attempt+1}/{max_retries}: Kafka not ready, retrying...")
                time.sleep(3)
            except Exception as e:
                print(f"Kafka connection error: {e}")
                time.sleep(3)
        return None

    def _consume(self):
        consumer = self._connect_kafka()
        if not consumer:
            print("Live consumer could not connect to Kafka. Will fall back to MongoDB polling.")
            self.connected = False
            return

        self.connected = True
        try:
            for msg in consumer:
                if not self.running:
                    break
                with self.lock:
                    self.messages.append(msg.value)
                    if len(self.messages) > 500:
                        self.messages.pop(0)
        except Exception as e:
            print(f"Consumer error: {e}")
        finally:
            consumer.close()

    def get_latest(self, n=200):
        with self.lock:
            return self.messages[-n:] if self.messages else []

if 'consumer' not in st.session_state:
    st.session_state.consumer = LiveIndicatorConsumer()

# --- Export Helper ---
def export_data(df, fmt='csv'):
    # Work on a copy to avoid modifying original DataFrame
    export_df = df.copy()
    # Remove timezone info from any datetime columns
    for col in export_df.select_dtypes(include=['datetimetz']).columns:
        export_df[col] = export_df[col].dt.tz_localize(None)

    if fmt == 'csv':
        data = export_df.to_csv(index=False)
        mime = 'text/csv'
        ext = 'csv'
    elif fmt == 'excel':
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as w:
            export_df.to_excel(w, index=False)
        data = output.getvalue()
        mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        ext = 'xlsx'
    else:
        return None
    b64 = base64.b64encode(data.encode() if isinstance(data, str) else data).decode()
    return f'<a href="data:{mime};base64,{b64}" download="worldbank_data.{ext}">Download {ext.upper()}</a>'

# --- UI ---
st.sidebar.title("🌍 World Bank Indicators")
page = st.sidebar.radio("Page", ["Live Stream", "Historical Analysis"])

if page == "Live Stream":
    st.title("📡 Live Development Indicators")
    st_autorefresh(interval=10000, key="live_refresh")

    # Try to get data from live Kafka consumer
    live = st.session_state.consumer.get_latest(200)

    if live:
        df_live = pd.DataFrame(live)
        st.success(f"✅ Streaming real data via Kafka: {len(df_live)} records")
    else:
        # Fallback: fetch latest from MongoDB
        df_live = get_latest_from_mongo(200)
        if not df_live.empty:
            st.info("ℹ️ Live Kafka consumer initializing; showing latest records from MongoDB.")
        else:
            st.warning("⏳ Waiting for data... The producer fetches updates every 60 seconds.")
            st.info("The World Bank API provides annual data. Live updates reflect the latest available year for each indicator.")
            df_live = pd.DataFrame()

    if not df_live.empty:
        col1, col2 = st.columns(2)
        with col1:
            selected_countries = st.multiselect(
                "Countries",
                options=sorted(df_live['country_name'].unique()),
                default=sorted(df_live['country_name'].unique())[:3]
            )
        with col2:
            selected_indicators = st.multiselect(
                "Indicators",
                options=sorted(df_live['indicator_name'].unique()),
                default=sorted(df_live['indicator_name'].unique())[:2]
            )

        if selected_countries:
            df_live = df_live[df_live['country_name'].isin(selected_countries)]
        if selected_indicators:
            df_live = df_live[df_live['indicator_name'].isin(selected_indicators)]

        st.subheader("Latest Values by Indicator")
        latest_by_indicator = df_live.sort_values('timestamp').groupby(['country_name', 'indicator_name']).last().reset_index()

        for indicator in selected_indicators:
            st.markdown(f"**{indicator}**")
            cols = st.columns(min(len(selected_countries), 4))
            for i, country in enumerate(selected_countries[:4]):
                country_data = latest_by_indicator[(latest_by_indicator['country_name'] == country) & (latest_by_indicator['indicator_name'] == indicator)]
                if not country_data.empty:
                    val = country_data.iloc[0]['value']
                    unit = country_data.iloc[0]['unit']
                    year = country_data.iloc[0]['year']
                    with cols[i % 4]:
                        st.metric(f"{country}", f"{val:,.0f}" if val > 100 else f"{val:,.2f}", delta=f"Year: {year}")

        st.subheader("Cross-Country Comparison")
        if len(selected_indicators) > 0:
            fig_bar = px.bar(
                latest_by_indicator,
                x='country_name',
                y='value',
                color='indicator_name',
                barmode='group',
                title="Latest Indicator Values by Country"
            )
            st.plotly_chart(fig_bar, width='stretch')

        if df_live['timestamp'].nunique() > 1:
            st.subheader("Recent Updates")
            fig_line = px.line(
                df_live.sort_values('timestamp'),
                x='timestamp',
                y='value',
                color='country_name',
                line_dash='indicator_name',
                title="Values Over Time"
            )
            st.plotly_chart(fig_line, width='stretch')

        st.subheader("Recent Records")
        st.dataframe(df_live.sort_values('timestamp', ascending=False).head(20), width='stretch')
    else:
        st.info("No live data yet. Ensure the producer is running and has fetched data.")

elif page == "Historical Analysis":
    st.title("📊 Historical Indicator Analysis")

    hours = st.slider("Hours of data to load", 1, 168, 24)
    df_hist = get_historical(hours)

    if not df_hist.empty:
        col1, col2 = st.columns(2)
        with col1:
            hist_countries = st.multiselect(
                "Countries",
                options=sorted(df_hist['country_name'].unique()),
                default=sorted(df_hist['country_name'].unique())[:5]
            )
        with col2:
            hist_indicators = st.multiselect(
                "Indicators",
                options=sorted(df_hist['indicator_name'].unique()),
                default=sorted(df_hist['indicator_name'].unique())[:2]
            )

        if hist_countries:
            df_hist = df_hist[df_hist['country_name'].isin(hist_countries)]
        if hist_indicators:
            df_hist = df_hist[df_hist['indicator_name'].isin(hist_indicators)]

        st.subheader(f"Historical Data (Last {hours} Hours, {len(df_hist)} records)")

        fig_hist = px.line(
            df_hist.sort_values('timestamp'),
            x='timestamp',
            y='value',
            color='country_name',
            line_dash='indicator_name',
            title="Indicator Trends"
        )
        st.plotly_chart(fig_hist, width='stretch')

        if not df_hist.empty:
            st.subheader("Statistical Summary")
            summary = df_hist.groupby(['country_name', 'indicator_name'])['value'].agg(['mean', 'min', 'max', 'count']).round(2)
            st.dataframe(summary, width='stretch')

        st.subheader("Export Data")
        col1, col2, _ = st.columns([1,1,2])
        with col1:
            if st.button("📥 Export as CSV"):
                st.markdown(export_data(df_hist, 'csv'), unsafe_allow_html=True)
        with col2:
            if st.button("📥 Export as Excel"):
                st.markdown(export_data(df_hist, 'excel'), unsafe_allow_html=True)

        with st.expander("View All Historical Data"):
            st.dataframe(df_hist.sort_values('timestamp', ascending=False), width='stretch')
    else:
        st.warning("No historical data found. Ensure the consumer is running and storing data.")