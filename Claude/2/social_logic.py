"""
SOCIAL LOGIC - VERSIONE CORRETTA
Parsing robusto + Salvataggio corretto per Content/Demographics/Data
"""

import pandas as pd
import io
import re
from datetime import datetime
from database import get_connection

# ============ CONSTANTS ============
DATE_MAP = {
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4, "maggio": 5, "giugno": 6,
    "luglio": 7, "agosto": 8, "settembre": 9, "ottobre": 10, "novembre": 11, "dicembre": 12,
    "gen": 1, "feb": 2, "mar": 3, "apr": 4, "mag": 5, "giu": 6,
    "lug": 7, "ago": 8, "set": 9, "ott": 10, "nov": 11, "dic": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
}
CURRENT_YEAR = datetime.now().year

# ============ DATA RETRIEVAL ============

def get_data_health():
    """Recupera ultimo dato e tutte le stats"""
    conn = get_connection()
    try:
        last = conn.execute("SELECT MAX(date_recorded) FROM social_stats").fetchone()
        last_str = last[0] if last and last[0] else None
        
        query = """
        SELECT date_recorded, platform, metric_type, value, source_type
        FROM social_stats 
        ORDER BY date_recorded DESC 
        LIMIT 5000
        """
        df = pd.read_sql_query(query, conn)
        return last_str, df
    except Exception as e:
        print(f"Error get_data_health: {e}")
        return None, pd.DataFrame()
    finally:
        conn.close()

def get_content_health():
    """Recupera performance content"""
    conn = get_connection()
    try:
        query = """
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
        WHERE p.date_recorded = (
            SELECT MAX(date_recorded) 
            FROM posts_performance 
            WHERE post_id = i.post_id
        )
        ORDER BY p.views DESC
        LIMIT 200
        """
        return pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"Error get_content_health: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def get_file_upload_history():
    """Recupera storico upload"""
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT upload_date, filename, platform, status FROM upload_logs ORDER BY id DESC LIMIT 100",
            conn
        )
    except:
        return pd.DataFrame()
    finally:
        conn.close()

def check_file_log(filename, platform):
    """Controlla se file già caricato"""
    conn = get_connection()
    try:
        result = conn.execute(
            "SELECT upload_date FROM upload_logs WHERE filename=? AND platform=? AND status LIKE '%OK%' ORDER BY id DESC LIMIT 1",
            (filename, platform)
        ).fetchone()
        return (True, result[0]) if result else (False, None)
    except:
        return False, None
    finally:
        conn.close()

def log_upload_event(filename, platform, status):
    """Registra evento upload"""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO upload_logs (filename, platform, status) VALUES (?, ?, ?)",
            (filename, platform, status)
        )
        conn.commit()
    except Exception as e:
        print(f"Error logging upload: {e}")
    finally:
        conn.close()

# ============ PARSING HELPERS ============

def parse_smart_date(date_str):
    """Parse qualsiasi formato data"""
    if not isinstance(date_str, str):
        return None
    
    s = date_str.strip().lower()
    
    # ISO with timezone (Instagram)
    if 't' in s and '-' in s:
        try:
            return s.split('t')[0]
        except:
            pass
    
    # Already clean ISO
    if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
        return s
    
    # Italian textual (TikTok): "15 novembre 2024"
    match = re.search(r'(\d{1,2})\s+([a-z]+)(?:\s+(\d{4}))?', s)
    if match:
        day = int(match.group(1))
        month_str = match.group(2)
        year = int(match.group(3)) if match.group(3) else CURRENT_YEAR
        
        month = None
        for key, val in DATE_MAP.items():
            if key in month_str:
                month = val
                break
        
        if month:
            # Handle year boundary
            if datetime.now().month < 6 and month > 8:
                year -= 1
            
            try:
                return datetime(year, month, day).strftime('%Y-%m-%d')
            except:
                pass
    
    # Standard formats
    formats = [
        '%Y-%m-%d',
        '%d/%m/%Y',
        '%d-%m-%Y',
        '%m/%d/%Y',
        '%Y/%m/%d',
        '%d.%m.%Y'
    ]
    
    s_clean = s.split()[0]  # Take first part before space
    for fmt in formats:
        try:
            return datetime.strptime(s_clean, fmt).strftime('%Y-%m-%d')
        except:
            continue
    
    return None

def clean_number(raw_val, is_currency=False):
    """Parse qualsiasi formato numero"""
    s = str(raw_val).lower().strip()
    
    if not s or s in ['nan', 'none', '', 'n/a', '--']:
        return 0
    
    # Handle K, M suffixes
    multiplier = 1.0
    if 'k' in s:
        multiplier = 1000.0
        s = s.replace('k', '')
    elif 'm' in s:
        multiplier = 1000000.0
        s = s.replace('m', '')
    
    s = s.strip()
    
    # Handle currency vs count
    if is_currency:
        # Currency: 1.234,56 (EU) or 1,234.56 (US)
        if ',' in s and '.' in s:
            if s.rfind(',') > s.rfind('.'):
                # EU: 1.234,56
                s = s.replace('.', '').replace(',', '.')
            else:
                # US: 1,234.56
                s = s.replace(',', '')
        elif ',' in s:
            # Single comma: assume decimal
            s = s.replace(',', '.')
    else:
        # Count: 1.234 = 1234, 1,234 = 1234
        if '.' in s and re.search(r'\.\d{3}$', s):
            # 1.234 = thousands
            s = s.replace('.', '')
        s = s.replace(',', '.')
    
    # Remove non-numeric
    s = re.sub(r'[^\d\.]', '', s)
    
    try:
        val = float(s) * multiplier
        return val if is_currency else int(val)
    except:
        return 0

# ============ CSV LOADER ============

def smart_csv_loader(uploaded_file):
    """
    Carica CSV con encoding auto-detect e trova header reale
    
    Returns:
        df: DataFrame
        status: "OK" o errore
        file_type: Tipo rilevato
    """
    try:
        bytes_data = uploaded_file.getvalue()
        content = None
        
        # Try encodings
        for enc in ['utf-8', 'utf-16', 'utf-8-sig', 'latin-1', 'cp1252']:
            try:
                content = bytes_data.decode(enc)
                break
            except:
                continue
        
        if not content:
            return None, "Encoding Error", "ERROR"
        
        lines = content.splitlines()
        
        # Find separator
        sep = ','
        comma_count = sum(l.count(',') for l in lines[:10])
        semi_count = sum(l.count(';') for l in lines[:10])
        tab_count = sum(l.count('\t') for l in lines[:10])
        
        if tab_count > max(comma_count, semi_count):
            sep = '\t'
        elif semi_count > comma_count:
            sep = ';'
        
        # Find header row
        header_keywords = [
            'date', 'data', 'time', 'giorno',
            'video', 'post', 'link', 'permalink',
            'gender', 'sesso', 'uomini', 'donne',
            'territor', 'countr', 'città',
            'inserzione', 'campagn', 'impression',
            'follower', 'reach', 'view', 'like'
        ]
        
        header_row = 0
        for i, line in enumerate(lines[:50]):
            line_lower = line.lower()
            if sep not in line:
                continue
            if any(kw in line_lower for kw in header_keywords):
                header_row = i
                break
        
        # Read CSV
        data_io = io.StringIO(content)
        df = pd.read_csv(
            data_io,
            sep=sep,
            skiprows=header_row,
            dtype=str,
            on_bad_lines='skip',
            engine='python'
        )
        
        # Clean columns
        df.columns = [str(c).strip() for c in df.columns]
        df = df.loc[:, ~df.columns.str.contains('^Unnamed', case=False, na=False)]
        df = df.dropna(how='all')
        
        # Detect file type
        cols_str = ' '.join([c.lower() for c in df.columns])
        file_type = detect_file_type(df, cols_str, uploaded_file.name)
        
        return df, "OK", file_type
        
    except Exception as e:
        return None, f"Parse error: {str(e)}", "ERROR"

def detect_file_type(df, cols_str, filename):
    """Rileva tipo file da colonne e nome"""
    
    fn = filename.lower()
    
    # META ADS
    if "nome dell'inserzione" in cols_str and "speso" in cols_str:
        return "META_ADS"
    
    # CONTENT (posts/videos)
    if any(x in cols_str for x in ['video link', 'permalink', 'post time']):
        if any(x in cols_str for x in ['total views', 'views', 'visualizzazioni']):
            return "CONTENT"
    
    # DEMOGRAPHICS - Gender
    if ("uomini" in cols_str and "donne" in cols_str) or \
       ("gender" in cols_str and "distribution" in cols_str):
        return "DEMOGRAPHIC_GENDER"
    
    # DEMOGRAPHICS - Geo
    if any(x in cols_str for x in ['territor', 'countr', 'città', 'location']):
        if "distribution" in cols_str or len(df.columns) == 2:
            return "DEMOGRAPHIC_GEO"
    
    # TIME SERIES (default for followers, reach, etc.)
    if any(x in cols_str for x in ['date', 'data', 'time', 'giorno']):
        # Determine metric from filename if generic
        if "follower" in fn:
            return "TIMESERIES_FOLLOWERS"
        elif "reach" in fn or "copertura" in fn:
            return "TIMESERIES_REACH"
        elif "impression" in fn:
            return "TIMESERIES_IMPRESSIONS"
        elif "interazi" in fn or "interaction" in fn:
            return "TIMESERIES_INTERACTIONS"
        else:
            return "TIMESERIES_GENERIC"
    
    return "UNKNOWN"

# ============ SAVE BULK ============

def save_social_bulk(df, platform, file_type):
    """
    Salva dati nel database in base al file_type
    
    Returns:
        rows_saved: int
        message: str
    """
    conn = get_connection()
    processed = 0
    today = datetime.now().strftime('%Y-%m-%d')
    
    try:
        # ========== 1. META ADS ==========
        if file_type == "META_ADS":
            col_name = next((c for c in df.columns if "inserzione" in c.lower()), None)
            col_spend = next((c for c in df.columns if "spes" in c.lower()), None)
            col_imp = next((c for c in df.columns if "impression" in c.lower()), None)
            
            if col_name:
                for _, row in df.iterrows():
                    name = str(row[col_name])
                    spend = clean_number(row.get(col_spend, 0), is_currency=True) if col_spend else 0
                    impressions = clean_number(row.get(col_imp, 0)) if col_imp else 0
                    
                    if name and name != 'nan':
                        upsert_stat(conn, "Meta Ads", f"Spend - {name}", spend, today)
                        if impressions > 0:
                            upsert_stat(conn, "Meta Ads", f"Impressions - {name}", impressions, today)
                        processed += 1
        
        # ========== 2. CONTENT ==========
        elif file_type == "CONTENT":
            col_link = next((c for c in df.columns if "link" in c.lower()), None)
            col_pub = next((c for c in df.columns if "post time" in c.lower() or "publish" in c.lower()), None)
            col_title = next((c for c in df.columns if "title" in c.lower() or "caption" in c.lower()), None)
            col_views = next((c for c in df.columns if "view" in c.lower()), None)
            col_likes = next((c for c in df.columns if "like" in c.lower()), None)
            col_comments = next((c for c in df.columns if "comment" in c.lower()), None)
            col_shares = next((c for c in df.columns if "share" in c.lower() or "condivision" in c.lower()), None)
            
            if col_link and col_pub:
                for _, row in df.iterrows():
                    link = str(row[col_link])
                    
                    # Extract post ID
                    post_id = link
                    # TikTok: video/123456
                    match_tk = re.search(r'video/(\d+)', link)
                    # Instagram: /p/ABC123 or /reel/ABC123
                    match_ig = re.search(r'/(?:p|reel)/([^/?]+)', link)
                    
                    if match_tk:
                        post_id = match_tk.group(1)
                    elif match_ig:
                        post_id = match_ig.group(1)
                    
                    # Parse publish date
                    pub_date = parse_smart_date(str(row[col_pub]))
                    if not pub_date:
                        continue
                    
                    title = str(row[col_title])[:500] if col_title else ''
                    
                    # Insert inventory
                    try:
                        conn.execute(
                            "INSERT OR REPLACE INTO posts_inventory (post_id, platform, date_published, caption, link) VALUES (?,?,?,?,?)",
                            (post_id, platform, pub_date, title, link)
                        )
                    except Exception as e:
                        print(f"Error inserting inventory: {e}")
                        continue
                    
                    # Parse metrics
                    views = clean_number(row.get(col_views, 0)) if col_views else 0
                    likes = clean_number(row.get(col_likes, 0)) if col_likes else 0
                    comments = clean_number(row.get(col_comments, 0)) if col_comments else 0
                    shares = clean_number(row.get(col_shares, 0)) if col_shares else 0
                    
                    # Insert performance
                    conn.execute(
                        "DELETE FROM posts_performance WHERE post_id=? AND date_recorded=?",
                        (post_id, today)
                    )
                    conn.execute(
                        "INSERT INTO posts_performance (post_id, date_recorded, views, likes, comments, shares) VALUES (?,?,?,?,?,?)",
                        (post_id, today, views, likes, comments, shares)
                    )
                    processed += 1
        
        # ========== 3. DEMOGRAPHICS - GENDER ==========
        elif file_type == "DEMOGRAPHIC_GENDER":
            # Instagram pivot format
            if "uomini" in df.columns and "donne" in df.columns:
                age_col = df.columns[0]
                for _, row in df.iterrows():
                    age_group = str(row[age_col])
                    male = clean_number(row['uomini'])
                    female = clean_number(row['donne'])
                    
                    if age_group and age_group != 'nan':
                        upsert_stat(conn, platform, f"Audience Gender Male ({age_group})", male, today)
                        upsert_stat(conn, platform, f"Audience Gender Female ({age_group})", female, today)
                        processed += 1
            
            # TikTok format (Gender, Distribution)
            elif "gender" in df.columns.str.lower().tolist():
                col_gender = next((c for c in df.columns if "gender" in c.lower()), None)
                col_dist = next((c for c in df.columns if "distribution" in c.lower()), None)
                
                if col_gender and col_dist:
                    for _, row in df.iterrows():
                        gender = str(row[col_gender])
                        value = clean_number(row[col_dist])
                        
                        # If value < 1, it's percentage as decimal (0.65 = 65%)
                        if value < 1 and value > 0:
                            value = value * 100
                        
                        if gender and gender != 'nan':
                            upsert_stat(conn, platform, f"Audience Gender {gender}", value, today)
                            processed += 1
        
        # ========== 4. DEMOGRAPHICS - GEO ==========
        elif file_type == "DEMOGRAPHIC_GEO":
            if len(df.columns) >= 2:
                cat_col = df.columns[0]
                val_col = df.columns[1]
                
                for _, row in df.iterrows():
                    location = str(row[cat_col])
                    value = clean_number(row[val_col])
                    
                    # Handle percentage
                    if value < 1 and value > 0:
                        value = value * 100
                    
                    if location and location != 'nan':
                        upsert_stat(conn, platform, f"Audience Geo {location}", value, today)
                        processed += 1
        
        # ========== 5. TIME SERIES ==========
        elif file_type.startswith("TIMESERIES"):
            # Find date column
            date_col = next((c for c in df.columns if any(x in c.lower() for x in ['date', 'data', 'time', 'giorno'])), None)
            
            if not date_col:
                return 0, "No date column found"
            
            # Find value columns (all except date)
            value_cols = [c for c in df.columns if c != date_col]
            
            # Determine metric name
            if file_type == "TIMESERIES_FOLLOWERS":
                metric_base = "Followers"
            elif file_type == "TIMESERIES_REACH":
                metric_base = "Reach"
            elif file_type == "TIMESERIES_IMPRESSIONS":
                metric_base = "Impressions"
            elif file_type == "TIMESERIES_INTERACTIONS":
                metric_base = "Interactions"
            else:
                metric_base = value_cols[0].title() if value_cols else "Metric"
            
            for _, row in df.iterrows():
                date_val = parse_smart_date(str(row[date_col]))
                if not date_val:
                    continue
                
                # Save each value column
                for val_col in value_cols:
                    value = clean_number(row[val_col])
                    
                    # Use metric_base if only one column, otherwise use column name
                    if len(value_cols) == 1:
                        metric_name = metric_base
                    else:
                        metric_name = val_col.title().replace('_', ' ')
                    
                    upsert_stat(conn, platform, metric_name, value, date_val)
                    processed += 1
        
        conn.commit()
        return processed, "OK"
        
    except Exception as e:
        return 0, f"Save error: {str(e)}"
    finally:
        conn.close()

def upsert_stat(conn, platform, metric, value, date_val):
    """Insert or update stat"""
    try:
        conn.execute(
            "DELETE FROM social_stats WHERE platform=? AND metric_type=? AND date_recorded=?",
            (platform, metric, date_val)
        )
        conn.execute(
            "INSERT INTO social_stats (platform, metric_type, value, date_recorded, source_type) VALUES (?,?,?,?,?)",
            (platform, metric, float(value), date_val, 'csv_v2')
        )
    except Exception as e:
        print(f"Upsert error: {e}")

def delete_social_stat(stat_id):
    """Delete single stat"""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM social_stats WHERE id=?", (stat_id,))
        conn.commit()
    except:
        pass
    finally:
        conn.close()