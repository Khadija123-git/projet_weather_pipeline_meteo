import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.figure_factory as ff
import snowflake.connector
import numpy as np
import io

st.write("Streamlit version:", st.__version__)
st.set_page_config(page_title="Météo DataApp – Multi-Villes", layout="wide")
st.markdown("<h1 style='text-align:center; color:#22292f; font-size:3em;'>🌦️ Dashboard Météo – Suivi en Temps Réel</h1>", unsafe_allow_html=True)

st.markdown("""
    <style>
    .stApp, .block-container {background-color:#FFFFFF;}
    .stSidebar, .css-6qob1r, .st-emotion-cache-1aehpvj {background-color: #23282e !important; color: #f6f6f7;}
    h1, h2, h3 {color: #14d0ff;}
    .stMetric {background: #23282e !important; border-radius: 14px;}    
    .stButton>button {background: linear-gradient(90deg,#14d0ff,#fc7c14); color: white; border-radius: 13px;}
    </style>
""", unsafe_allow_html=True)



### --- DATA SNOWFLAKE ---

def get_connection():
    return snowflake.connector.connect(
        user="aya", password="Ayaayaayahamim2005?",
        account="CCNXNBR-ILB26280",
        warehouse="WH_INGESTION", database="KAFKA_DB", schema="ANALYTICS"
    )

@st.cache_data
def load_dim():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM KAFKA_DB.ANALYTICS.DIM_LOCATIONS", conn)
    conn.close()
    return df
@st.cache_data
def load_gold():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM KAFKA_DB.ANALYTICS.FCT_DAILY_WEATHER_STATS", conn)
    conn.close()
    return df
@st.cache_data
def load_fact():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM KAFKA_DB.ANALYTICS.FCT_WEATHER_MEASUREMENTS", conn)
    conn.close()
    return df

df_dim, df_gold, df_fact = load_dim(), load_gold(), load_fact()
df_gold["DATE_OBS"] = pd.to_datetime(df_gold["DATE_OBS"])
df_fact["DATE_OBS"] = pd.to_datetime(df_fact["DATE_OBS"])

### --- SIDEBAR : Choix villes, dates ---

villes_all = df_dim["VILLE"].unique().tolist()
with st.sidebar:
    st.markdown("<h2 style='color:#14d0ff'>🏙️ Filtres</h2>", unsafe_allow_html=True)
    villes_sel = st.multiselect("Villes à analyser", villes_all, default=villes_all[:3])
    dmin, dmax = df_gold["DATE_OBS"].min().date(), df_gold["DATE_OBS"].max().date()
    date_range = st.date_input("Plage de dates", (dmin, dmax), min_value=dmin, max_value=dmax)
    dstart, dend = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])

# -- Merge des tables pour récupérer coords et loc_key
gold = df_gold[df_gold["VILLE"].isin(villes_sel) & (df_gold["DATE_OBS"]>=dstart) & (df_gold["DATE_OBS"]<=dend)].merge(
    df_dim[["VILLE","LOCATION_KEY","LATITUDE","LONGITUDE"]], on=["VILLE","LOCATION_KEY"], how="left"
)
fact = df_fact[df_fact["LOCATION_KEY"].isin(gold["LOCATION_KEY"]) & (df_fact["DATE_OBS"]>=dstart) & (df_fact["DATE_OBS"]<=dend)].merge(
    df_dim[["LOCATION_KEY","VILLE"]], on="LOCATION_KEY", how="left"
)

### --- KPIs dynamiques ---
st.markdown("<h2>🌤️ Indicateurs clés</h2>", unsafe_allow_html=True)
kpi_row = st.columns(len(villes_sel) if villes_sel else 1)
for i, ville in enumerate(villes_sel):
    d = gold[gold["VILLE"] == ville]
    temp = d['AVG_TEMP_C'].mean()
    humid = d['AVG_HUMIDITY'].mean()
    amp = d['THERMAL_AMPLITUDE'].mean()
    color = "#fc7c14" if temp>38 else "#14d0ff"
    score_confort = np.clip(100 - abs(22-temp)*2 - abs(50-humid) - (amp if amp>15 else 0), 0, 100)
    msg = "🔥" if temp > 38 else "🌡️"
    kpi_row[i].markdown(
        f"""<div style='background:{color};padding:1em .6em;border-radius:12px;text-align:center;color:#fff'>
        <b>{ville}</b><br>{msg} {temp:.1f}°C<br>💧 {humid:.1f}%<br>Δ {amp:.1f}°C<br>
        Score confort: <b style="color:#fff;background:#2228e0;border-radius:7px;padding:2px 10px">{score_confort:.0f}/100</b></div>""", unsafe_allow_html=True
    )

### --- BAR CHART : Temp et humidité ---
colsT = st.columns(2)
with colsT[0]:
    st.subheader("🌡️ Classement températures moyennes")
    st.plotly_chart(
        px.bar(gold.groupby("VILLE")["AVG_TEMP_C"].mean().sort_values(ascending=False).reset_index(),
            x="VILLE", y="AVG_TEMP_C", color="AVG_TEMP_C",
            color_continuous_scale="YlOrRd", template="plotly_dark"), use_container_width=True)
with colsT[1]:
    st.subheader("💧 Classement humidité moyenne")
    st.plotly_chart(
        px.bar(gold.groupby("VILLE")["AVG_HUMIDITY"].mean().sort_values(ascending=False).reset_index(),
            x="VILLE", y="AVG_HUMIDITY", color="AVG_HUMIDITY",
            color_continuous_scale="Blues", template="plotly_dark"), use_container_width=True)

### --- PIE/DOUGHNUT PLUIE ---
st.subheader("🌧️ Proportion d'observations de pluie")
pie_df = fact.groupby("VILLE")["IS_RAIN"].sum().reset_index()
fig_pie = px.pie(pie_df, values="IS_RAIN", names="VILLE",
                 color="VILLE", title="Part des observations pluvieuses", hole=.5, template="plotly_dark")
st.plotly_chart(fig_pie, use_container_width=True)

### --- HISTOGRAMME sur températures horaires ---
st.subheader("🕗 Histogramme températures horaires (toutes villes)")
hist_df = fact
fig_hist = px.histogram(hist_df, x="TEMP_C", color="VILLE", nbins=30, barmode="overlay", template="plotly_dark")
fig_hist.update_traces(opacity=0.7)
st.plotly_chart(fig_hist, use_container_width=True)

### --- HEATMAP température vs humidité ---
st.subheader("☀️🌡️ Heatmap Température vs Humidité")
if not hist_df.empty:
    heatmap_prep = hist_df.groupby(["TEMP_C","HUMIDITE_PCT"]).size().reset_index(name='counts')
    fig_heatmap = px.density_heatmap(
        heatmap_prep, x='TEMP_C', y='HUMIDITE_PCT', z='counts',
        nbinsx=25, nbinsy=15, color_continuous_scale="Turbo", template="plotly_dark")
    st.plotly_chart(fig_heatmap, use_container_width=True)
else:
    st.info("Pas assez de données pour la heatmap.")

### --- RADAR PLOT PAR VILLE ---
st.subheader("🌪️ Radar météo par ville")
from math import pi

radar_cols = ["AVG_TEMP_C", "AVG_HUMIDITY", "THERMAL_AMPLITUDE"]
def radar_trace(row, cols):
    values = [row[c] if not pd.isna(row[c]) else 0 for c in cols]
    values += values[:1] # boucler
    return values

radar_stats = gold.groupby("VILLE")[radar_cols].mean().reset_index()
radar_fig = px.line_polar()
categories = [c.replace("_", " ").title() for c in radar_cols]
for i, row in radar_stats.iterrows():
    radar_fig.add_scatterpolar(
        r=radar_trace(row, radar_cols), theta=categories,
        fill='toself', name=row["VILLE"]
    )
radar_fig.update_layout(template="plotly_dark", polar=dict(
    radialaxis=dict(visible=True,range=[0,max(gold[radar_cols].max())+5])
))
st.plotly_chart(radar_fig, use_container_width=True)

### --- BOXPLOT TEMP HUMIDITE ---
st.subheader("📦 Boxplot Température et Humidité (multi-villes)")
fig_box = px.box(fact, x="VILLE", y="TEMP_C", color="VILLE", points="all", template="plotly_dark", title="Température par ville")
st.plotly_chart(fig_box, use_container_width=True)
fig_box_h = px.box(fact, x="VILLE", y="HUMIDITE_PCT", color="VILLE", points="outliers", template="plotly_dark", title="Humidité par ville")
st.plotly_chart(fig_box_h, use_container_width=True)

### --- MAPBOX ---
st.subheader("🗺️ Carte météo interactive (température & vent)")
agg_map = gold.groupby("VILLE").agg({
    "AVG_TEMP_C":"mean", "LATITUDE":"first", "LONGITUDE":"first"
}).reset_index()
agg_map = agg_map.merge(
    fact.groupby("VILLE")["WIND_KMH"].mean().reset_index(), on="VILLE", how="left"
)
fig_map = px.scatter_mapbox(
    agg_map, lat="LATITUDE", lon="LONGITUDE", hover_name="VILLE",
    size="WIND_KMH", color="AVG_TEMP_C",
    color_continuous_scale="Turbo", zoom=2, height=420, mapbox_style="carto-darkmatter",
    title="Carte : température (couleur) et vent (taille du point)"
)
fig_map.update_traces(marker=dict(sizemin=8, opacity=0.84))
st.plotly_chart(fig_map, use_container_width=True)

### --- TÉLÉCHARGEMENT DU DASHBOARD/DATA ---
st.markdown("## 💾 Exporter les données analysées")
excel_buff = io.BytesIO()
for df_to_fix in [gold, fact]:
    for col in ["DATE_OBS", "TIMESTAMP_BRUT"]:
        if col in df_to_fix.columns:
            df_to_fix[col] = pd.to_datetime(df_to_fix[col]).dt.tz_localize(None)
with pd.ExcelWriter(excel_buff, engine='xlsxwriter') as writer:
    gold.to_excel(writer, sheet_name="Gold", index=False)
    fact.to_excel(writer, sheet_name="Fact", index=False)
# PAS de writer.save() ici !!
excel_buff.seek(0)
st.download_button("⬇️ Télécharger les données filtrées (Excel)", data=excel_buff,
                   file_name="dashboard_meteo_data.xlsx", mime="application/vnd.ms-excel")

