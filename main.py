"""
MAIN.PY - STREAMLIT APP CORRETTA
Con logica fix per visualizzare Content, Demographics, Data
"""

import streamlit as st
import pandas as pd
import uuid
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import time

# IMPORT MODULI
from database import init_advanced_db, get_connection
from social_logic import smart_csv_loader, save_social_bulk, get_data_health, check_file_log, log_upload_event, get_content_health

# Optional imports (create empty files if missing)
try:
    from knowledge_logic import ingest_local_pdfs, scrape_webpage, save_knowledge, get_knowledge_context
except:
    def ingest_local_pdfs(): return "Module not available"
    def scrape_webpage(u): return None, "Module not available"
    def save_knowledge(s,c): pass
    def get_knowledge_context(): return ""

try:
    from campaign_logic import get_campaigns, save_campaign
except:
    def get_campaigns(): return pd.DataFrame()
    def save_campaign(d): pass

try:
    from spotify_client import SpotifyAPI
except:
    class SpotifyAPI:
        def __init__(self): self.tok = None
        def data(self): return "Module not available"
        def save(self,i,s): pass
        def get_auth(self): return ""
        def get_tok(self,c): pass

# ============ STREAMLIT CONFIG ============

st.set_page_config(page_title="YANGKIDD ENTERPRISE OS", page_icon="💎", layout="wide")

st.markdown("""
<style>
    .stApp { 
        background: linear-gradient(135deg, #0a0a0a 0%, #1a0a1a 100%); 
        color: #e0e0e0; 
    }
    .stTextInput > div > div > input { 
        background-color: #1a1a1a; 
        color: #00ff99; 
        border: 2px solid #333; 
    }
    [data-testid="stDataFrame"] { 
        background-color: #1a1a1a; 
    }
    h1, h2, h3 { 
        color: #00ff99 !important; 
    }
    .success-box {
        background-color: rgba(0, 255, 153, 0.1);
        border: 1px solid #00ff99;
        border-radius: 5px;
        padding: 10px;
        margin: 10px 0;
    }
    .error-box {
        background-color: rgba(255, 59, 48, 0.1);
        border: 1px solid #ff3b30;
        border-radius: 5px;
        padding: 10px;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# ============ INIT DATABASE ============

init_advanced_db()

# ============ SESSION STATE ============

if 'init' not in st.session_state:
    st.session_state.update({
        'init': True,
        'messages': [],
        'thinking': False
    })

# ============ SIDEBAR ============

with st.sidebar:
    st.title("💎 ENTERPRISE OS")
    st.caption("YangKidd Music Marketing System")
    
    nav = st.radio(
        "MENU",
        ["📈 Social Tracker", "💬 Strategy", "📚 Knowledge", "🔌 API", "⚙️ Ads"],
        key="main_nav"
    )
    
    st.divider()
    st.header("🔍 Filtri Globali")
    
    # Get all platforms from DB
    _, df_stats_all = get_data_health()
    df_content_all = get_content_health()
    
    plats_1 = df_stats_all['platform'].unique().tolist() if not df_stats_all.empty else []
    plats_2 = df_content_all['platform'].unique().tolist() if not df_content_all.empty else []
    available_plats = sorted(list(set(plats_1 + plats_2)))
    
    if available_plats:
        sel_plats = st.multiselect(
            "Piattaforme Visualizzate", 
            available_plats, 
            default=available_plats,
            key="platform_filter"
        )
    else:
        sel_plats = []
        st.caption("⚠️ Nessun dato. Carica file CSV.")

# ============ SOCIAL TRACKER PAGE ============

if nav == "📈 Social Tracker":
    st.title("📈 Social Data Warehouse")
    
    # Quick Debug Button
    if st.button("🔍 Run Quick Diagnostic", help="Check database content"):
        with st.spinner("Running diagnostic..."):
            conn = get_connection()
            
            diag_results = {
                'social_stats': conn.execute("SELECT COUNT(*) FROM social_stats").fetchone()[0],
                'posts_inventory': conn.execute("SELECT COUNT(*) FROM posts_inventory").fetchone()[0],
                'posts_performance': conn.execute("SELECT COUNT(*) FROM posts_performance").fetchone()[0],
                'upload_logs': conn.execute("SELECT COUNT(*) FROM upload_logs").fetchone()[0]
            }
            
            # Check if content join works
            try:
                content_join = pd.read_sql_query("""
                    SELECT COUNT(*) as count FROM posts_inventory i
                    JOIN posts_performance p ON i.post_id = p.post_id
                """, conn)
                diag_results['content_join'] = content_join.iloc[0]['count']
            except:
                diag_results['content_join'] = 0
            
            conn.close()
            
            # Display results
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("📊 Social Stats", diag_results['social_stats'])
            col2.metric("📝 Content Items", diag_results['posts_inventory'])
            col3.metric("📈 Performance", diag_results['posts_performance'])
            col4.metric("🔗 Join Result", diag_results['content_join'])
            col5.metric("📋 Upload Logs", diag_results['upload_logs'])
            
            # Status message
            if diag_results['posts_inventory'] == 0:
                st.error("❌ No content in database. Upload content CSV files.")
            elif diag_results['content_join'] == 0:
                st.warning("⚠️ Content exists but JOIN fails. Check post_id format.")
            elif diag_results['content_join'] > 0:
                st.success(f"✅ System OK! {diag_results['content_join']} content items ready to display.")
    
    st.divider()
    
    # Data health check
    last_date, _ = get_data_health()
    today = datetime.now().strftime('%Y-%m-%d')
    
    col_status1, col_status2 = st.columns(2)
    
    with col_status1:
        if not last_date:
            st.markdown('<div class="error-box">⚠️ <strong>Database Vuoto</strong><br>Carica i primi CSV per iniziare.</div>', unsafe_allow_html=True)
        elif last_date < today:
            st.markdown(f'<div class="error-box">⚠️ <strong>Dati Non Aggiornati</strong><br>Ultimo dato: {last_date}</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="success-box">✅ <strong>Dati Aggiornati</strong><br>Ultimo dato: {last_date}</div>', unsafe_allow_html=True)
    
    with col_status2:
        # Count data points
        conn = get_connection()
        stats_count = conn.execute("SELECT COUNT(*) FROM social_stats").fetchone()[0]
        content_count = conn.execute("SELECT COUNT(*) FROM posts_inventory").fetchone()[0]
        conn.close()
        
        st.metric("📊 Data Points", stats_count)
        st.metric("🎬 Content Items", content_count)
    
    st.divider()
    
    # ========== UPLOAD SECTION ==========
    
    with st.expander("📂 Upload CSV Files", expanded=True):
        st.markdown("### Carica i tuoi file CSV")
        st.caption("Supporta Instagram, TikTok, Meta Ads - Encoding automatico")
        
        if "uploader_key" not in st.session_state:
            st.session_state["uploader_key"] = str(uuid.uuid4())
        
        up_files = st.file_uploader(
            "Trascina qui i file CSV",
            accept_multiple_files=True,
            key=st.session_state["uploader_key"],
            type=['csv', 'txt']
        )
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            plat = st.selectbox(
                "Piattaforma",
                ["Instagram", "TikTok", "Facebook", "YouTube", "Meta Ads"],
                key="platform_select"
            )
        
        with col2:
            force = st.checkbox("Forza Reload", help="Ricarica anche file già processati")
        
        with col3:
            st.metric("File selezionati", len(up_files) if up_files else 0)
        
        if st.button("🚀 ELABORA FILE", type="primary", use_container_width=True):
            if not up_files:
                st.warning("Seleziona almeno un file!")
            else:
                processed_count = 0
                error_count = 0
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                results = []
                
                for i, file in enumerate(up_files):
                    status_text.text(f"Processing {file.name}...")
                    
                    # Check if already processed
                    exists, last_upload = check_file_log(file.name, plat)
                    
                    if exists and not force:
                        results.append({
                            'file': file.name,
                            'status': '⏭️ Skipped',
                            'reason': f'Already processed on {last_upload}',
                            'rows': 0
                        })
                        continue
                    
                    # Parse CSV
                    df, msg, file_type = smart_csv_loader(file)
                    
                    if df is None:
                        results.append({
                            'file': file.name,
                            'status': '❌ Error',
                            'reason': msg,
                            'rows': 0
                        })
                        error_count += 1
                    else:
                        # Save to DB
                        rows, save_msg = save_social_bulk(df, plat, file_type)
                        
                        if rows > 0:
                            log_upload_event(file.name, plat, f"OK ({rows} rows, type: {file_type})")
                            results.append({
                                'file': file.name,
                                'status': '✅ Success',
                                'reason': f'Type: {file_type}',
                                'rows': rows
                            })
                            processed_count += 1
                        else:
                            results.append({
                                'file': file.name,
                                'status': '⚠️ Warning',
                                'reason': save_msg,
                                'rows': 0
                            })
                            error_count += 1
                    
                    progress_bar.progress((i + 1) / len(up_files))
                
                status_text.empty()
                progress_bar.empty()
                
                # Show results
                st.markdown("### 📊 Upload Results")
                
                if processed_count > 0:
                    st.success(f"✅ Successfully processed {processed_count} file(s)")
                
                if error_count > 0:
                    st.error(f"❌ {error_count} file(s) had errors")
                
                # Results table
                results_df = pd.DataFrame(results)
                st.dataframe(results_df, use_container_width=True, hide_index=True)
                
                if processed_count > 0:
                    time.sleep(2)
                    st.session_state["uploader_key"] = str(uuid.uuid4())
                    st.rerun()
    
    st.divider()
    
    # ========== VISUALIZATION TABS ==========
    
    st.subheader("📊 Data Visualization")
    
    # Prepare filtered data
    df_stats_filtered = pd.DataFrame()
    df_content_filtered = pd.DataFrame()
    df_time = pd.DataFrame()
    df_demo = pd.DataFrame()
    
    if not df_stats_all.empty and sel_plats:
        df_stats_filtered = df_stats_all[df_stats_all['platform'].isin(sel_plats)].copy()
        df_stats_filtered['date_recorded'] = pd.to_datetime(df_stats_filtered['date_recorded'])
        
        # Separate time series from demographics
        is_demo = df_stats_filtered['metric_type'].str.contains("Audience", case=False, na=False)
        df_time = df_stats_filtered[~is_demo]
        df_demo = df_stats_filtered[is_demo]
    
    if not df_content_all.empty and sel_plats:
        df_content_filtered = df_content_all[df_content_all['platform'].isin(sel_plats)].copy()
    
    tab1, tab2, tab3, tab4 = st.tabs(["📉 Trend Analysis", "🎬 Content Performance", "👥 Demographics", "🔢 Raw Data"])
    
    # ========== TAB 1: TREND ANALYSIS ==========
    with tab1:
        st.markdown("### 📈 Time Series Metrics")
        
        if df_time.empty:
            st.info("📭 No time series data available. Upload CSV files with date-based metrics.")
        else:
            # Metric selector
            col1, col2 = st.columns([1, 3])
            
            with col1:
                available_metrics = sorted(df_time['metric_type'].unique().tolist())
                
                # Filter out hourly metrics by default
                main_metrics = [m for m in available_metrics if not any(x in m for x in ['H0', 'H1', 'H2'])]
                
                selected_metrics = st.multiselect(
                    "Select Metrics (Max 2)",
                    available_metrics,
                    default=main_metrics[:2] if len(main_metrics) >= 2 else available_metrics[:2],
                    max_selections=2,
                    key="metric_selector"
                )
            
            with col2:
                if selected_metrics:
                    df_plot = df_time[df_time['metric_type'].isin(selected_metrics)]
                    
                    if len(selected_metrics) == 1:
                        # Single metric - simple line chart
                        fig = px.line(
                            df_plot,
                            x='date_recorded',
                            y='value',
                            color='platform',
                            markers=True,
                            template="plotly_dark",
                            title=selected_metrics[0]
                        )
                        fig.update_layout(
                            xaxis_title="Date",
                            yaxis_title="Value",
                            hovermode='x unified'
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    
                    else:
                        # Two metrics - dual axis
                        fig = make_subplots(specs=[[{"secondary_y": True}]])
                        
                        colors = ['#00ff99', '#ff00ff', '#00ccff', '#ffff00']
                        metric1, metric2 = selected_metrics[0], selected_metrics[1]
                        
                        for i, platform in enumerate(sel_plats):
                            # Metric 1 - primary axis
                            df1 = df_plot[(df_plot['platform'] == platform) & (df_plot['metric_type'] == metric1)]
                            if not df1.empty:
                                fig.add_trace(
                                    go.Scatter(
                                        x=df1['date_recorded'],
                                        y=df1['value'],
                                        name=f"{platform} - {metric1}",
                                        line=dict(color=colors[i % 4], width=3)
                                    ),
                                    secondary_y=False
                                )
                            
                            # Metric 2 - secondary axis
                            df2 = df_plot[(df_plot['platform'] == platform) & (df_plot['metric_type'] == metric2)]
                            if not df2.empty:
                                fig.add_trace(
                                    go.Scatter(
                                        x=df2['date_recorded'],
                                        y=df2['value'],
                                        name=f"{platform} - {metric2}",
                                        line=dict(color=colors[i % 4], width=2, dash='dot')
                                    ),
                                    secondary_y=True
                                )
                        
                        fig.update_layout(
                            template="plotly_dark",
                            hovermode="x unified",
                            title="Dual Metric Comparison"
                        )
                        fig.update_xaxes(title_text="Date")
                        fig.update_yaxes(title_text=metric1, secondary_y=False)
                        fig.update_yaxes(title_text=metric2, secondary_y=True)
                        
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Select at least one metric to visualize")
    
    # ========== TAB 2: CONTENT PERFORMANCE ==========
    with tab2:
        st.markdown("### 🎬 Post & Video Performance")
        
        # DEBUG INFO
        with st.expander("🔍 Debug Info", expanded=False):
            conn = get_connection()
            inv_count = conn.execute("SELECT COUNT(*) FROM posts_inventory").fetchone()[0]
            perf_count = conn.execute("SELECT COUNT(*) FROM posts_performance").fetchone()[0]
            conn.close()
            
            st.write(f"**posts_inventory rows:** {inv_count}")
            st.write(f"**posts_performance rows:** {perf_count}")
            
            if inv_count == 0:
                st.error("❌ No data in posts_inventory - Content CSV not uploaded or parsed incorrectly")
            if perf_count == 0:
                st.error("❌ No data in posts_performance - Performance data not saved")
            
            if inv_count > 0 and perf_count > 0 and df_content_filtered.empty:
                st.error("❌ Data exists but JOIN query fails - post_id mismatch")
                
                # Show sample data
                conn = get_connection()
                sample_inv = pd.read_sql_query("SELECT * FROM posts_inventory LIMIT 3", conn)
                sample_perf = pd.read_sql_query("SELECT * FROM posts_performance LIMIT 3", conn)
                conn.close()
                
                st.write("**Sample posts_inventory:**")
                st.dataframe(sample_inv, use_container_width=True)
                
                st.write("**Sample posts_performance:**")
                st.dataframe(sample_perf, use_container_width=True)
        
        if df_content_filtered.empty:
            st.info("📭 No content data available.")
            
            # Try alternative query without subquery
            conn = get_connection()
            try:
                # Simplified query
                alt_query = """
                SELECT i.post_id, 
                       i.platform, 
                       i.date_published, 
                       i.caption, 
                       i.link,
                       p.views, 
                       p.likes, 
                       p.comments, 
                       p.shares,
                       p.date_recorded
                FROM posts_inventory i
                JOIN posts_performance p ON i.post_id = p.post_id
                ORDER BY p.views DESC
                LIMIT 200
                """
                
                df_alt = pd.read_sql_query(alt_query, conn)
                
                if not df_alt.empty:
                    st.success(f"✅ Found {len(df_alt)} content items using alternative query!")
                    
                    # Filter by platform
                    if sel_plats:
                        df_alt = df_alt[df_alt['platform'].isin(sel_plats)]
                    
                    if not df_alt.empty:
                        # Scatter plot
                        fig = px.scatter(
                            df_alt,
                            x='date_published',
                            y='views',
                            size='likes',
                            color='platform',
                            hover_data=['caption', 'comments', 'shares'],
                            template="plotly_dark",
                            title="Content Performance Over Time"
                        )
                        fig.update_layout(
                            xaxis_title="Published Date",
                            yaxis_title="Views"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Top performing content
                        st.markdown("#### 🏆 Top Performing Content")
                        
                        top_content = df_alt.sort_values('views', ascending=False).head(10)
                        
                        # Format display
                        display_df = top_content[['platform', 'date_published', 'caption', 'views', 'likes', 'comments', 'shares']].copy()
                        display_df['caption'] = display_df['caption'].apply(lambda x: str(x)[:80] + '...' if len(str(x)) > 80 else str(x))
                        
                        st.dataframe(display_df, use_container_width=True, hide_index=True)
                else:
                    st.warning("Upload content CSV files (with columns: Post time, Video link, Total views, Total likes)")
                    
                    st.markdown("""
                    **Expected CSV format:**
                    ```
                    Post time,Video link,Video title,Total views,Total likes
                    2024-11-15T14:23:00,https://tiktok.com/video/123,My Video,1200,234
                    ```
                    
                    **Or Instagram format:**
                    ```
                    Post time,Permalink,Total views,Total likes
                    2024-11-15T14:23:00,https://instagram.com/p/ABC123,5000,456
                    ```
                    """)
            except Exception as e:
                st.error(f"Query error: {e}")
            finally:
                conn.close()
        else:
            st.caption(f"Showing {len(df_content_filtered)} posts/videos")
            
            # Scatter plot
            fig = px.scatter(
                df_content_filtered,
                x='date_published',
                y='views',
                size='likes',
                color='platform',
                hover_data=['caption', 'comments', 'shares'],
                template="plotly_dark",
                title="Content Performance Over Time"
            )
            fig.update_layout(
                xaxis_title="Published Date",
                yaxis_title="Views"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Top performing content
            st.markdown("#### 🏆 Top Performing Content")
            
            top_content = df_content_filtered.sort_values('views', ascending=False).head(10)
            
            # Format display
            display_df = top_content[['platform', 'date_published', 'caption', 'views', 'likes', 'comments', 'shares']].copy()
            display_df['caption'] = display_df['caption'].apply(lambda x: str(x)[:80] + '...' if len(str(x)) > 80 else str(x))
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    # ========== TAB 3: DEMOGRAPHICS ==========
    with tab3:
        st.markdown("### 👥 Audience Demographics")
        
        if df_demo.empty:
            st.info("📭 No demographic data available. Upload audience demographic CSV files.")
        else:
            # Get latest snapshot
            latest_date = df_demo['date_recorded'].max()
            df_demo_latest = df_demo[df_demo['date_recorded'] == latest_date]
            
            st.caption(f"Snapshot date: {latest_date}")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### 👤 Gender Distribution")
                
                df_gender = df_demo_latest[df_demo_latest['metric_type'].str.contains("Gender", case=False)]
                
                if not df_gender.empty:
                    df_gender['label'] = df_gender['metric_type'].str.replace("Audience Gender ", "")
                    
                    fig = px.pie(
                        df_gender,
                        values='value',
                        names='label',
                        facet_col='platform',
                        template="plotly_dark",
                        title="Gender Split by Platform"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No gender data")
            
            with col2:
                st.markdown("#### 🌍 Geographic Distribution")
                
                df_geo = df_demo_latest[df_demo_latest['metric_type'].str.contains("Geo", case=False)]
                
                if not df_geo.empty:
                    df_geo['label'] = df_geo['metric_type'].str.replace("Audience Geo ", "")
                    
                    fig = px.bar(
                        df_geo,
                        x='label',
                        y='value',
                        color='platform',
                        barmode='group',
                        template="plotly_dark",
                        title="Top Locations"
                    )
                    fig.update_layout(xaxis_title="Location", yaxis_title="Followers/Percentage")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No geographic data")
    
    # ========== TAB 4: RAW DATA ==========
    with tab4:
        st.markdown("### 🔢 Raw Data Table")
        
        if df_stats_filtered.empty:
            st.info("No data available")
        else:
            # Add filters
            col1, col2, col3 = st.columns(3)
            
            with col1:
                filter_platform = st.multiselect(
                    "Filter Platform",
                    df_stats_filtered['platform'].unique().tolist(),
                    default=df_stats_filtered['platform'].unique().tolist(),
                    key="raw_platform_filter"
                )
            
            with col2:
                filter_metric = st.multiselect(
                    "Filter Metric",
                    df_stats_filtered['metric_type'].unique().tolist(),
                    key="raw_metric_filter"
                )
            
            with col3:
                sort_by = st.selectbox(
                    "Sort By",
                    ["date_recorded", "value", "metric_type"],
                    key="raw_sort"
                )
            
            # Apply filters
            df_display = df_stats_filtered.copy()
            
            if filter_platform:
                df_display = df_display[df_display['platform'].isin(filter_platform)]
            
            if filter_metric:
                df_display = df_display[df_display['metric_type'].isin(filter_metric)]
            
            df_display = df_display.sort_values(sort_by, ascending=False)
            
            st.dataframe(df_display, use_container_width=True, height=400)
            
            # Export button
            csv = df_display.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"yangkidd_data_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
    
    st.divider()
    
    # ========== DANGER ZONE ==========
    with st.expander("🗑️ Danger Zone", expanded=False):
        st.warning("⚠️ **WARNING:** These actions cannot be undone!")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🗑️ RESET All Data", type="secondary"):
                conn = get_connection()
                conn.execute("DELETE FROM social_stats")
                conn.execute("DELETE FROM upload_logs")
                conn.execute("DELETE FROM posts_inventory")
                conn.execute("DELETE FROM posts_performance")
                conn.commit()
                conn.close()
                st.success("✅ Database cleared")
                time.sleep(1)
                st.rerun()
        
        with col2:
            platform_to_delete = st.selectbox(
                "Delete Single Platform",
                [""] + (sel_plats if sel_plats else []),
                key="delete_platform_select"
            )
            
            if st.button("Delete Platform Data") and platform_to_delete:
                conn = get_connection()
                conn.execute("DELETE FROM social_stats WHERE platform=?", (platform_to_delete,))
                conn.execute("DELETE FROM posts_inventory WHERE platform=?", (platform_to_delete,))
                conn.commit()
                conn.close()
                st.success(f"✅ Deleted {platform_to_delete} data")
                time.sleep(1)
                st.rerun()

# ========== OTHER PAGES (Stub) ==========

elif nav == "💬 Strategy":
    st.title("🧠 Strategy Room")
    st.info("AI Chat module - Connect Ollama or external AI service")
    st.caption("Feature coming soon or implement custom AI logic")

elif nav == "📚 Knowledge":
    st.title("📚 Knowledge Base")
    st.info("PDF/Web scraping module for knowledge management")
    st.caption("Feature coming soon")

elif nav == "🔌 API":
    st.title("🔌 API Connections")
    st.info("Spotify/Instagram/TikTok API integration")
    st.caption("Feature coming soon")

elif nav == "⚙️ Ads":
    st.title("⚙️ Campaign Manager")
    st.info("Ads campaign tracking and ROI calculator")
    st.caption("Feature coming soon")