import streamlit as st
import pandas as pd
import threading
import time
import uuid
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime

# IMPORT MODULI
from database import init_advanced_db, get_connection
from social_logic import smart_csv_loader, detect_metric_from_filename, save_social_bulk, get_data_health, check_file_log, log_upload_event, get_file_upload_history, get_content_health
from knowledge_logic import ingest_local_pdfs, scrape_webpage, save_knowledge, get_knowledge_context
from campaign_logic import get_campaigns, save_campaign
from spotify_client import SpotifyAPI
from ai_engine import ai_thread, load_chat_history, save_chat_message, clear_chat_history

st.set_page_config(page_title="YANGKIDD ENTERPRISE OS", page_icon="💎", layout="wide")
st.markdown("""
<style>
    .stApp { background: linear-gradient(135deg, #0a0a0a 0%, #1a0a1a 100%); color: #e0e0e0; }
    .stTextInput > div > div > input { background-color: #1a1a1a; color: #00ff99; border: 2px solid #333; }
    [data-testid="stDataFrame"] { background-color: #1a1a1a; }
    .metric-card { background: #222; padding: 15px; border-radius: 10px; border-left: 5px solid #00ff99; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
    h1, h2, h3 { color: #00ff99 !important; }
</style>
""", unsafe_allow_html=True)

init_advanced_db()
if 'init' not in st.session_state:
    try: hist = load_chat_history()
    except: hist = []
    st.session_state.update({'init':True, 'messages':hist, 'thinking':False})

# --- SIDEBAR ---
with st.sidebar:
    st.title("💎 ENTERPRISE OS")
    nav = st.radio("MENU", ["📈 Social Tracker", "💬 Strategy", "📚 Knowledge", "🔌 API", "⚙️ Ads"])
    
    st.divider()
    st.header("🔍 Filtri Globali")
    
    _, df_stats_all = get_data_health()
    if not df_stats_all.empty: df_stats_all.columns = ['date_recorded', 'platform', 'metric_type', 'value']
    df_content_all = get_content_health()
    
    plats = sorted(list(set(df_stats_all['platform'].unique().tolist() + df_content_all['platform'].unique().tolist()))) if not df_stats_all.empty else []
    
    if plats:
        sel_plats = st.multiselect("Piattaforme", plats, default=plats)
    else:
        sel_plats = []
        st.caption("Carica dati per i filtri.")

# --- TRACKER ---
if nav == "📈 Social Tracker":
    st.title("📈 Social Data Warehouse")
    last, _ = get_data_health()
    if not last: st.warning("⚠️ DB Vuoto")
    else: st.success(f"✅ Dati al {last}")

    # TABS
    t1, t2, t3, t4 = st.tabs(["📉 Trend", "🎬 Content", "👥 Demografica", "🔢 Dati"])
    
    # 1. TREND
    with t1:
        if not df_stats_all.empty:
            df_filt = df_stats_all[df_stats_all['platform'].isin(sel_plats)]
            is_demo = df_filt['metric_type'].str.contains("Audience", case=False, na=False)
            df_trend = df_filt[~is_demo]
            
            if not df_trend.empty:
                mets = sorted(df_trend['metric_type'].unique())
                m_sel = st.multiselect("Metriche", mets, default=mets[:2], max_selections=2)
                
                if len(m_sel) > 0:
                    df_p = df_trend[df_trend['metric_type'].isin(m_sel)]
                    fig = px.line(df_p, x='date_recorded', y='value', color='platform', line_dash='metric_type', template="plotly_dark")
                    st.plotly_chart(fig, use_container_width=True)
            else: st.info("Nessun dato trend per la selezione.")
        else: st.info("Carica CSV.")

    # 2. CONTENT
    with t2:
        if not df_content_all.empty:
            df_c_filt = df_content_all[df_content_all['platform'].isin(sel_plats)]
            if not df_c_filt.empty:
                fig = px.scatter(df_c_filt, x='date_published', y='views', size='likes', color='platform', hover_data=['caption'], template="plotly_dark", size_max=60)
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df_c_filt.sort_values('views', ascending=False).head(10)[['platform','caption','views','likes']], use_container_width=True)
            else: st.info("Nessun contenuto per la selezione.")
        else: st.info("Carica file Content.")

    # 3. DEMO
    with t3:
        if not df_stats_all.empty:
            df_filt = df_stats_all[df_stats_all['platform'].isin(sel_plats)]
            is_demo = df_filt['metric_type'].str.contains("Audience", case=False, na=False)
            df_demo = df_filt[is_demo]
            
            if not df_demo.empty:
                lat = df_demo['date_recorded'].max()
                df_s = df_demo[df_demo['date_recorded'] == lat]
                
                c1, c2 = st.columns(2)
                with c1:
                    dg = df_s[df_s['metric_type'].str.contains("Gender")]
                    if not dg.empty:
                        dg['label'] = dg['metric_type'].str.replace("Audience Gender ", "")
                        st.plotly_chart(px.pie(dg, values='value', names='label', title="Genere", template="plotly_dark"), use_container_width=True)
                with c2:
                    dgeo = df_s[df_s['metric_type'].str.contains("Geo")]
                    if not dgeo.empty:
                        dgeo['label'] = dgeo['metric_type'].str.replace("Audience Geo ", "")
                        st.plotly_chart(px.bar(dgeo, x='label', y='value', title="Geo", template="plotly_dark"), use_container_width=True)
            else: st.info("Nessun dato demografico.")

    with t4: st.dataframe(df_stats_all, use_container_width=True)

    # UPLOAD
    with st.expander("📂 Upload", expanded=True):
        if "ukey" not in st.session_state: st.session_state["ukey"] = str(uuid.uuid4())
        up = st.file_uploader("CSV", accept_multiple_files=True, key=st.session_state["ukey"])
        c1, c2 = st.columns(2)
        plat = c1.selectbox("Piattaforma", ["Instagram", "TikTok", "Facebook", "Meta Ads"])
        force = c2.checkbox("Forza")
        
        if st.button("🚀 Elabora"):
            cnt = 0
            bar = st.progress(0)
            for i, f in enumerate(up):
                ex, _ = check_file_log(f.name, plat)
                if ex and not force: st.toast(f"Saltato {f.name}")
                else:
                    m = detect_metric_from_filename(f.name)
                    df, msg = smart_csv_loader(f)
                    if df is not None:
                        rows, msg = save_social_bulk(df, plat, m)
                        if rows > 0: 
                            log_upload_event(f.name, plat, f"OK {rows}")
                            cnt += 1
                            st.toast(f"✅ {f.name}: {rows}")
                        else: st.error(f"❌ {f.name}: 0 righe ({msg})")
                    else: st.error(f"❌ {f.name}: {msg}")
                bar.progress((i+1)/len(up))
            
            if cnt > 0:
                st.success("Fatto!")
                time.sleep(1)
                st.session_state["ukey"] = str(uuid.uuid4())
                st.rerun()

    if st.button("🗑️ RESET"):
        conn = get_connection()
        conn.execute("DELETE FROM social_stats"); conn.execute("DELETE FROM upload_logs")
        conn.execute("DELETE FROM posts_inventory"); conn.execute("DELETE FROM posts_performance")
        conn.commit(); conn.close()
        st.warning("Reset!")
        time.sleep(1); st.rerun()

# --- ALTRE PAGINE (Standard) ---
elif nav == "💬 Strategy":
    st.title("🧠 Strategy")
    if st.button("Clear"): clear_chat_history(); st.session_state.messages=[]; st.rerun()
    for m in st.session_state.messages: st.chat_message(m["role"]).write(m["content"])
    if p:=st.chat_input():
        st.session_state.messages.append({"role":"user","content":p})
        save_chat_message("user",p)
        threading.Thread(target=ai_thread, args=(st.session_state.messages,"","", "", None)).start()
        st.rerun()

elif nav == "📚 Knowledge":
    st.title("Knowledge")
    t1, t2 = st.tabs(["PDF", "Web"])
    with t1: st.write(ingest_local_pdfs() if st.button("Scan") else "")
    with t2:
        u = st.text_input("URL")
        if st.button("Scrape") and u: save_knowledge("WEB: "+u, scrape_webpage(u)[1]); st.success("OK")

elif nav == "🔌 API":
    st.title("API")
    s=SpotifyAPI(); st.write(s.data() if s.tok else "No Token")
    if not s.tok:
        i=st.text_input("ID"); c=st.text_input("Secret")
        if st.button("Save"): s.save(i,c)
        if s.cid: st.markdown(f"[Login]({s.get_auth()})")
    if "code" in st.query_params: s.get_tok(st.query_params["code"]); st.rerun()

elif nav == "⚙️ Ads":
    st.title("Ads")
    with st.form("ads"):
        n=st.text_input("Name"); s=st.number_input("Spend"); r=st.number_input("Rev")
        if st.form_submit_button("Save"): save_campaign({'name':n,'platform':'Meta','spend':s,'revenue':r,'impressions':0,'streams':0}); st.rerun()
    st.dataframe(get_campaigns())