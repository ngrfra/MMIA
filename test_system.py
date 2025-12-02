"""
SOCIAL_LOGIC.PY - VERSIONE FINALE CORRETTA
Fixes: Date parsing, Demographics recognition, Instagram CSV parsing
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

# ============ PARSING HELPERS ============

def parse_smart_date(date_str):
    """Parse qualsiasi formato data - FIX ANNO"""
    if not isinstance(date_str, str):
        return None
    
    s = date_str.strip().lower()
    current_year = datetime.now().year
    current_month = datetime.now().month
    
    # ISO with timezone (Instagram)
    if 't' in s and '-' in s:
        try:
            return s.split('t')[0]
        except:
            pass
    
    # Already clean ISO
    if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
        return s
    
    # Italian textual (TikTok): "15 novembre 2024" or "15 novembre"
    match = re.search(r'(\d{1,2})\s+([a-z]+)(?:\s+(\d{4}))?', s)
    if match:
        day = int(match.group(1))
        month_str = match.group(2)
        year_specified = match.group(3)
        
        # Find month
        month = None
        for key, val in DATE_MAP.items():
            if key in month_str:
                month = val
                break
        
        if month:
            # CRITICAL FIX: Determine correct year
            if year_specified:
                year = int(year_specified)
            else:
                # No year specified - infer from month
                # If current month is Jan-Feb and data month is Oct-Dec, it's LAST year
                # If current month is Nov-Dec and data month is Jan-Feb, it's NEXT year (rare)
                if current_month <= 2 and month >= 10:
                    year = current_year - 1  # Ex: We're in Jan 2025, data says "15 novembre" → Nov 2024
                elif current_month >= 11 and month <= 2:
                    year = current_year  # Ex: We're in Dec 2024, data says "15 gennaio" → Jan 2024
                else:
                    year = current_year  # Same year
            
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
    
    s_clean = s.split()[0]
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
        if ',' in s and '.' in s:
            if s.rfind(',') > s.rfind('.'):
                s = s.replace('.', '').replace(',', '.')
            else:
                s = s.replace(',', '')
        elif ',' in s:
            s = s.replace(',', '.')
    else:
        if '.' in s and re.search(r'\.\d{3}$', s):
            s = s.replace('.', '')
        s = s.replace(',', '.')
    
    s = re.sub(r'[^\d\.]', '', s)
    
    try:
        val = float(s) * multiplier
        return val if is_currency else int(val)
    except:
        return 0

# ============ DATA RETRIEVAL ============

def get_data_health():
    """Recupera ultimo dato e stats"""
    conn = get_connection()
    try:
        last = conn.execute("SELECT MAX(date_recorded) FROM social_stats").fetchone()
        last_str = last[0] if last and last[0] else None
        
        query = "SELECT * FROM social_stats ORDER BY date_recorded DESC LIMIT 5000"
        df = pd.read_sql_query(query, conn)
        return last_str, df
    except:
        return None, pd.DataFrame()
    finally:
        conn.close()

def get_content_health():
    """Recupera performance content"""
    conn = get_connection()
    try:
        query = """
        SELECT i.post_id, i.platform, i.date_published, i.caption, i.link,
               p.views, p.likes, p.comments, p.shares, p.date_recorded
        FROM posts_inventory i
        JOIN posts_performance p ON i.post_id = p.post_id
        WHERE p.date_recorded = (
            SELECT MAX(date_recorded) FROM posts_performance WHERE post_id = i.post_id
        )
        ORDER BY p.views DESC LIMIT 200
        """
        return pd.read_sql_query(query, conn)
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
        conn.execute("INSERT INTO upload_logs (filename, platform, status) VALUES (?, ?, ?)",
                    (filename, platform, status))
        conn.commit()
    except:
        pass
    finally:
        conn.close()

# ============ CSV LOADER ============

def smart_csv_loader(uploaded_file):
    """Carica CSV con encoding auto-detect"""
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
            'gender', 'sesso', 'uomini', 'donne', 'maschi', 'femmine',
            'territor', 'countr', 'città', 'paes',
            'inserzione', 'campagn', 'impression',
            'follower', 'reach', 'view', 'like', 'copertura', 'interazi'
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
        df = pd.read_csv(data_io, sep=sep, skiprows=header_row, dtype=str,
                        on_bad_lines='skip', engine='python')
        
        # Clean
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
    """Rileva tipo file - FIX DEMOGRAPHICS"""
    
    fn = filename.lower()
    
    # META ADS
    if "nome dell'inserzione" in cols_str and "speso" in cols_str:
        return "META_ADS"
    
    # CONTENT (posts/videos)
    if any(x in cols_str for x in ['video link', 'permalink', 'post time']):
        if any(x in cols_str for x in ['total views', 'views', 'visualizzazioni', 'total likes']):
            return "CONTENT"
    
    # DEMOGRAPHICS - Gender (FIX: Riconosci pivot Instagram)
    # Instagram format: Age range columns + Uomini/Donne rows
    if len(df.columns) >= 3:  # At least: Age, Males, Females
        # Check if column names look like age ranges
        age_pattern = r'\d{2}-\d{2}|\d{2}\+'
        has_age_cols = any(re.search(age_pattern, str(col)) for col in df.columns)
        
        # Check for gender keywords in first column or as column names
        gender_keywords = ['uomini', 'donne', 'maschi', 'femmine', 'male', 'female']
        has_gender = any(kw in cols_str for kw in gender_keywords)
        
        if has_age_cols or (has_gender and len(df.columns) >= 2):
            return "DEMOGRAPHIC_GENDER"
    
    # TikTok gender format
    if "gender" in cols_str and "distribution" in cols_str:
        return "DEMOGRAPHIC_GENDER"
    
    # DEMOGRAPHICS - Geo
    geo_keywords = ['territor', 'countr', 'città', 'location', 'paese', 'paes']
    if any(x in cols_str for x in geo_keywords):
        if "distribution" in cols_str or len(df.columns) == 2:
            return "DEMOGRAPHIC_GEO"
    
    # TIME SERIES (Instagram metrics)
    if any(x in cols_str for x in ['date', 'data', 'time', 'giorno']):
        # Determine metric from filename
        if "follower" in fn:
            return "TIMESERIES_FOLLOWERS"
        elif "reach" in fn or "copertura" in fn:
            return "TIMESERIES_REACH"
        elif "impression" in fn:
            return "TIMESERIES_IMPRESSIONS"
        elif "interazi" in fn or "interaction" in fn:
            return "TIMESERIES_INTERACTIONS"
        elif "visit" in fn or "visite" in fn:
            return "TIMESERIES_VISITS"
        elif "clic" in fn or "click" in fn:
            return "TIMESERIES_CLICKS"
        elif "visual" in fn:
            return "TIMESERIES_VIEWS"
        else:
            return "TIMESERIES_GENERIC"
    
    return "UNKNOWN"

# ============ SAVE BULK ============

def save_social_bulk(df, platform, file_type):
    """Salva dati nel database"""
    conn = get_connection()
    processed = 0
    today = datetime.now().strftime('%Y-%m-%d')
    
    try:
        # ========== META ADS ==========
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
        
        # ========== CONTENT ==========
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
                    match_tk = re.search(r'video/(\d+)', link)
                    match_ig = re.search(r'/(?:p|reel)/([^/?]+)', link)
                    
                    if match_tk:
                        post_id = match_tk.group(1)
                    elif match_ig:
                        post_id = match_ig.group(1)
                    
                    # Parse publish date (FIXED)
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
                    except:
                        continue
                    
                    # Parse metrics
                    views = clean_number(row.get(col_views, 0)) if col_views else 0
                    likes = clean_number(row.get(col_likes, 0)) if col_likes else 0
                    comments = clean_number(row.get(col_comments, 0)) if col_comments else 0
                    shares = clean_number(row.get(col_shares, 0)) if col_shares else 0
                    
                    # Insert performance
                    conn.execute("DELETE FROM posts_performance WHERE post_id=? AND date_recorded=?",
                               (post_id, today))
                    conn.execute(
                        "INSERT INTO posts_performance (post_id, date_recorded, views, likes, comments, shares) VALUES (?,?,?,?,?,?)",
                        (post_id, today, views, likes, comments, shares)
                    )
                    processed += 1
        
        # ========== DEMOGRAPHICS - GENDER (FIX) ==========
        elif file_type == "DEMOGRAPHIC_GENDER":
            # Instagram pivot format (Age ranges as columns, gender as rows)
            if len(df.columns) >= 3 and any(re.search(r'\d{2}-\d{2}|\d{2}\+', str(col)) for col in df.columns):
                # Find age columns
                age_cols = [c for c in df.columns if re.search(r'\d{2}-\d{2}|\d{2}\+', str(c))]
                
                # Iterate rows (should be Male/Female)
                for _, row in df.iterrows():
                    gender_label = str(row.iloc[0]).lower()
                    
                    # Determine if Male or Female
                    if any(x in gender_label for x in ['uomini', 'maschi', 'male', 'm']):
                        gender = "Male"
                    elif any(x in gender_label for x in ['donne', 'femmine', 'female', 'f']):
                        gender = "Female"
                    else:
                        gender = gender_label.title()
                    
                    # Process each age group
                    for age_col in age_cols:
                        value = clean_number(row[age_col])
                        if value > 0:
                            metric_name = f"Audience Gender {gender} ({age_col})"
                            upsert_stat(conn, platform, metric_name, value, today)
                            processed += 1
            
            # TikTok format (Gender, Distribution columns)
            elif "gender" in df.columns[0].lower():
                col_gender = df.columns[0]
                col_dist = df.columns[1]
                
                for _, row in df.iterrows():
                    gender = str(row[col_gender]).title()
                    value = clean_number(row[col_dist])
                    
                    if value < 1 and value > 0:
                        value = value * 100
                    
                    if gender and gender != 'Nan':
                        upsert_stat(conn, platform, f"Audience Gender {gender}", value, today)
                        processed += 1
        
        # ========== DEMOGRAPHICS - GEO ==========
        elif file_type == "DEMOGRAPHIC_GEO":
            if len(df.columns) >= 2:
                cat_col = df.columns[0]
                val_col = df.columns[1]
                
                for _, row in df.iterrows():
                    location = str(row[cat_col])
                    value = clean_number(row[val_col])
                    
                    if value < 1 and value > 0:
                        value = value * 100
                    
                    if location and location != 'nan':
                        upsert_stat(conn, platform, f"Audience Geo {location}", value, today)
                        processed += 1
        
        # ========== TIME SERIES (FIX: Instagram CSV) ==========
        elif file_type.startswith("TIMESERIES"):
            date_col = next((c for c in df.columns if any(x in c.lower() for x in ['date', 'data', 'time', 'giorno'])), None)
            
            if not date_col:
                return 0, "No date column found"
            
            value_cols = [c for c in df.columns if c != date_col]
            
            # Determine metric name
            metric_map = {
                "TIMESERIES_FOLLOWERS": "Followers",
                "TIMESERIES_REACH": "Reach",
                "TIMESERIES_IMPRESSIONS": "Impressions",
                "TIMESERIES_INTERACTIONS": "Interactions",
                "TIMESERIES_VISITS": "Profile Visits",
                "TIMESERIES_CLICKS": "Link Clicks",
                "TIMESERIES_VIEWS": "Profile Views"
            }
            
            metric_base = metric_map.get(file_type, "Metric")
            
            for _, row in df.iterrows():
                date_val = parse_smart_date(str(row[date_col]))
                if not date_val:
                    continue
                
                for val_col in value_cols:
                    value = clean_number(row[val_col])
                    
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
            (platform, metric, float(value), date_val, 'csv_v3')
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