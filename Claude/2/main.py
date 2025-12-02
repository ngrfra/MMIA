"""
MAIN.PY - CSV to Human-Readable Text Converter
Semplice convertitore CSV in testo leggibile
"""

import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
import io
import re

# ============ STREAMLIT CONFIG ============

st.set_page_config(page_title="CSV to Text Converter", page_icon="📄", layout="wide")

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
    h1, h2, h3 { 
        color: #00ff99 !important; 
    }
    .text-output {
        background-color: #1a1a1a;
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #333;
        font-family: 'Courier New', monospace;
        white-space: pre-wrap;
        max-height: 600px;
        overflow-y: auto;
    }
</style>
""", unsafe_allow_html=True)

# ============ CSV LOADER ============

def load_csv_simple(uploaded_file):
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
            return None, "Errore di encoding"
        
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
        header_keywords = ['date', 'data', 'time', 'giorno', 'video', 'post', 'link', 
                          'gender', 'sesso', 'territor', 'countr', 'follower', 'view', 'like']
        
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
        
        return df, "OK"
        
    except Exception as e:
        return None, f"Errore: {str(e)}"

# ============ CSV TO TEXT CONVERTER ============

def parse_numeric_value(value):
    """Converte valore in numero"""
    if pd.isna(value) or value == '':
        return None
    
    s = str(value).strip().lower()
    s_clean = re.sub(r'[^\d.,km]', '', s)
    
    try:
        if 'k' in s_clean:
            num = float(re.sub(r'[^\d.]', '', s_clean.replace('k', ''))) * 1000
        elif 'm' in s_clean:
            num = float(re.sub(r'[^\d.]', '', s_clean.replace('m', ''))) * 1000000
        else:
            # Gestisci formato italiano
            if ',' in s_clean and '.' in s_clean:
                if s_clean.rfind(',') > s_clean.rfind('.'):
                    s_clean = s_clean.replace('.', '').replace(',', '.')
            elif ',' in s_clean:
                s_clean = s_clean.replace(',', '.')
            num = float(re.sub(r'[^\d.]', '', s_clean))
        return num
    except:
        return None

def format_number(value):
    """Formatta numeri in modo leggibile e compatto"""
    if pd.isna(value) or value == '' or str(value).lower() in ['nan', 'none', 'n/a', '--']:
        return "—"
    
    num = parse_numeric_value(value)
    if num is None:
        return str(value)[:15]  # Limita testo
    
    # Formattazione compatta
    if num >= 1000000000:  # Miliardi
        return f"{num/1000000000:.2f}B"
    elif num >= 1000000:  # Milioni
        return f"{num/1000000:.2f}M"
    elif num >= 1000:  # Migliaia
        return f"{num/1000:.1f}K"
    elif num >= 1:
        return f"{num:,.0f}"
    else:
        return f"{num:.2f}"

def format_date(value):
    """Formatta date in modo leggibile e compatto"""
    if pd.isna(value) or value == '':
        return "—"
    
    s = str(value).strip()
    
    # ISO format
    if 'T' in s:
        try:
            dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
            return dt.strftime('%d/%m/%Y')
        except:
            pass
    
    # Prova altri formati comuni
    for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%m/%d/%Y']:
        try:
            dt = datetime.strptime(s.split()[0], fmt)
            return dt.strftime('%d/%m/%Y')
        except:
            continue
    
    return s[:12]  # Limita lunghezza

def detect_file_type(df, filename):
    """Rileva il tipo di file per applicare formattazione specifica"""
    fn_lower = filename.lower()
    cols_str = ' '.join([c.lower() for c in df.columns])
    
    # Instagram serie temporali
    if any(x in fn_lower for x in ['clic', 'copertura', 'follower', 'interazioni', 'visite', 'visualizzazioni']):
        if 'data' in cols_str and 'primary' in cols_str:
            return "INSTAGRAM_TIMESERIES"
    
    # Meta Ads
    if any(x in fn_lower for x in ['inserzioni', 'eta_destinazi', 'giorno_ora', 'tlp_inserz']):
        if 'importo speso' in cols_str or 'impression' in cols_str:
            return "META_ADS"
    
    # TikTok Content
    if 'content' in fn_lower or ('video' in cols_str and 'total views' in cols_str):
        return "TIKTOK_CONTENT"
    
    # Demografici
    if 'pubblico' in fn_lower or ('uomini' in cols_str and 'donne' in cols_str):
        return "DEMOGRAPHICS"
    
    # TikTok Demografici specifici
    if 'followeractivity' in fn_lower:
        return "TIKTOK_FOLLOWER_ACTIVITY"
    if 'followerhistory' in fn_lower:
        return "TIKTOK_FOLLOWER_HISTORY"
    if 'followergender' in fn_lower or 'followertop' in fn_lower:
        return "TIKTOK_DEMOGRAPHICS"
    if 'viewers' in fn_lower and 'tiktok' in fn_lower:
        return "TIKTOK_VIEWERS"
    if 'overview' in fn_lower and 'tiktok' in fn_lower:
        return "TIKTOK_OVERVIEW"
    
    return "GENERIC"

def csv_to_readable_text(df, filename=""):
    """Converte DataFrame in testo leggibile e intuitivo"""
    
    if df.empty:
        return "⚠️ Il file CSV è vuoto o non contiene dati validi."
    
    file_type = detect_file_type(df, filename)
    output = []
    
    # Header intuitivo
    output.append("=" * 80)
    output.append(f"📄 {filename}")
    output.append("=" * 80)
    output.append("")
    
    # ========== INSTAGRAM SERIE TEMPORALI ==========
    if file_type == "INSTAGRAM_TIMESERIES":
        date_col = next((c for c in df.columns if 'data' in c.lower() or 'date' in c.lower()), None)
        value_col = next((c for c in df.columns if 'primary' in c.lower() or c.lower() not in ['data', 'date']), None)
        
        if date_col and value_col:
            # Estrai metriche chiave
            values = []
            dates = []
            for _, row in df.iterrows():
                date_val = format_date(row[date_col])
                num_val = parse_numeric_value(row[value_col])
                if num_val is not None and date_val != "—":
                    values.append(num_val)
                    dates.append(date_val)
            
            if values:
                total = sum(values)
                avg = total / len(values)
                max_val = max(values)
                min_val = min(values)
                max_idx = values.index(max_val)
                min_idx = values.index(min_val)
                
                # Trend
                if len(values) > 1:
                    first_half = sum(values[:len(values)//2]) / (len(values)//2)
                    second_half = sum(values[len(values)//2:]) / (len(values) - len(values)//2)
                    trend = "📈 Crescita" if second_half > first_half * 1.1 else "📉 Calo" if second_half < first_half * 0.9 else "➡️ Stabile"
                else:
                    trend = "—"
                
                output.append(f"📊 ANALISI: {filename.split('/')[-1].replace('.csv', '')}")
                output.append("")
                output.append(f"   Periodo: {dates[0] if dates else '—'} → {dates[-1] if dates else '—'}")
                output.append(f"   Giorni analizzati: {len(values)}")
                output.append("")
                output.append("   📈 PERFORMANCE:")
                output.append(f"      • Totale: {format_number(total)}")
                output.append(f"      • Media giornaliera: {format_number(avg)}")
                output.append(f"      • Picco massimo: {format_number(max_val)} ({dates[max_idx] if max_idx < len(dates) else '—'})")
                output.append(f"      • Valore minimo: {format_number(min_val)} ({dates[min_idx] if min_idx < len(dates) else '—'})")
                output.append(f"      • Trend: {trend}")
                output.append("")
                
                # Ultimi 7 giorni
                if len(values) >= 7:
                    output.append("   📅 ULTIMI 7 GIORNI:")
                    for i in range(max(0, len(values)-7), len(values)):
                        output.append(f"      {dates[i] if i < len(dates) else '—':<12} → {format_number(values[i]):>10}")
                    output.append("")
    
    # ========== META ADS ==========
    elif file_type == "META_ADS":
        # Estrai metriche chiave
        spend_col = next((c for c in df.columns if 'speso' in c.lower() or 'spend' in c.lower()), None)
        imp_col = next((c for c in df.columns if 'impression' in c.lower() and 'totali' not in c.lower()), None)
        click_col = next((c for c in df.columns if 'clic' in c.lower() and 'link' in c.lower()), None)
        roas_col = next((c for c in df.columns if 'roas' in c.lower()), None)
        cpm_col = next((c for c in df.columns if 'cpm' in c.lower()), None)
        
        # Identifica colonne per filtrare righe di riepilogo
        name_col = next((c for c in df.columns if 'nome' in c.lower() and 'inserzione' in c.lower()), None)
        ora_col = next((c for c in df.columns if 'ora' in c.lower() and 'giorno' in c.lower()), None)
        eta_col = next((c for c in df.columns if 'età' in c.lower() or 'age' in c.lower()), None)
        dest_col = next((c for c in df.columns if 'destinazione' in c.lower()), None)
        
        # Cerca riga di riepilogo (riga con valori grandi ma campi chiave vuoti)
        summary_row = None
        if spend_col and imp_col:
            for idx, row in df.iterrows():
                spend_val = parse_numeric_value(row[spend_col]) or 0
                imp_val = parse_numeric_value(row[imp_col]) or 0
                # Se ha valori grandi ma campi chiave vuoti, è probabilmente un totale
                name_val = str(row[name_col]).strip() if name_col else ""
                ora_val = str(row[ora_col]).strip() if ora_col else ""
                eta_val = str(row[eta_col]).strip() if eta_col else ""
                
                is_empty = (not name_val or name_val == "" or name_val.lower() == "nan") and \
                          (not ora_val or ora_val == "" or ora_val.lower() == "nan") and \
                          (not eta_val or eta_val == "" or eta_val.lower() == "nan")
                
                if is_empty and spend_val > 50 and imp_val > 1000:
                    summary_row = row
                    break
        
        total_spend = 0
        total_imp = 0
        total_clicks = 0
        total_roas = 0
        roas_count = 0
        
        # Se abbiamo trovato una riga di riepilogo, usala
        if summary_row is not None:
            if spend_col:
                total_spend = parse_numeric_value(summary_row[spend_col]) or 0
            if imp_col:
                total_imp = parse_numeric_value(summary_row[imp_col]) or 0
            if click_col:
                total_clicks = parse_numeric_value(summary_row[click_col]) or 0
            if roas_col:
                roas_val = parse_numeric_value(summary_row[roas_col])
                if roas_val and roas_val > 0:
                    total_roas = roas_val
                    roas_count = 1
        else:
            # Altrimenti, somma solo le righe dettagliate (escludi riepiloghi)
            for _, row in df.iterrows():
                # Escludi righe di riepilogo
                name_val = str(row[name_col]).strip() if name_col else ""
                ora_val = str(row[ora_col]).strip() if ora_col else ""
                eta_val = str(row[eta_col]).strip() if eta_col else ""
                dest_val = str(row[dest_col]).strip() if dest_col else ""
                
                # Skip righe con campi chiave vuoti (sono riepiloghi)
                if (not name_val or name_val == "" or name_val.lower() == "nan") and \
                   (not ora_val or ora_val == "" or ora_val.lower() == "nan") and \
                   (not eta_val or eta_val == "" or eta_val.lower() == "nan"):
                    continue
                
                # Skip righe con "Tutte le..." o "Nessun dettaglio" (sono totali parziali)
                if dest_val and ("tutte le" in dest_val.lower() or "nessun dettaglio" in dest_val.lower()):
                    continue
                
                if spend_col:
                    total_spend += parse_numeric_value(row[spend_col]) or 0
                if imp_col:
                    total_imp += parse_numeric_value(row[imp_col]) or 0
                if click_col:
                    total_clicks += parse_numeric_value(row[click_col]) or 0
                if roas_col:
                    roas_val = parse_numeric_value(row[roas_col])
                    if roas_val and roas_val > 0:
                        total_roas += roas_val
                        roas_count += 1
        
        output.append(f"💰 CAMPAGNA: {filename.split('/')[-1].replace('.csv', '')}")
        output.append("")
        output.append("   💵 PERFORMANCE:")
        output.append(f"      • Spesa totale: €{format_number(total_spend)}")
        output.append(f"      • Impression: {format_number(total_imp)}")
        output.append(f"      • Clic: {format_number(total_clicks)}")
        if total_imp > 0:
            ctr_calc = (total_clicks / total_imp) * 100
            output.append(f"      • CTR: {ctr_calc:.2f}%")
        if total_clicks > 0:
            cpc = total_spend / total_clicks
            output.append(f"      • CPC: €{cpc:.3f}")
        if total_imp > 0 and total_spend > 0:
            cpm_calc = (total_spend / total_imp) * 1000
            output.append(f"      • CPM: €{cpm_calc:.2f}")
        if roas_count > 0:
            avg_roas = total_roas / roas_count
            output.append(f"      • ROAS medio: {avg_roas:.2f}x")
        output.append("")
        
        # Analisi per ora del giorno (se presente)
        if ora_col and spend_col:
            output.append("   ⏰ PERFORMANCE PER FASCIA ORARIA (Top 5):")
            ora_stats = {}
            for _, row in df.iterrows():
                # Escludi righe di riepilogo
                ora = str(row[ora_col]).strip() if ora_col else ""
                name_val = str(row[name_col]).strip() if name_col else ""
                
                # Skip se ora è vuota o se è una riga di riepilogo
                if not ora or ora == "" or ora.lower() == "nan":
                    continue
                if not name_val or name_val == "" or name_val.lower() == "nan":
                    continue
                
                spend = parse_numeric_value(row[spend_col]) or 0
                clicks = parse_numeric_value(row[click_col]) or 0 if click_col else 0
                if spend > 0:
                    if ora not in ora_stats:
                        ora_stats[ora] = {'spend': 0, 'clicks': 0}
                    ora_stats[ora]['spend'] += spend
                    ora_stats[ora]['clicks'] += clicks
            
            sorted_ora = sorted(ora_stats.items(), key=lambda x: x[1]['spend'], reverse=True)
            for i, (ora, stats) in enumerate(sorted_ora[:5], 1):
                ctr_ora = (stats['clicks'] / total_imp * 100) if total_imp > 0 else 0
                output.append(f"      {i}. {ora:<20} | €{format_number(stats['spend']):>8} | CTR: {ctr_ora:.2f}%")
            output.append("")
        
        # Top inserzioni
        if name_col and spend_col:
            top_ads = []
            for _, row in df.iterrows():
                name = str(row[name_col]).strip() if name_col else ""
                # Escludi righe con nome vuoto (riepiloghi)
                if not name or name == "" or name.lower() == "nan":
                    continue
                
                spend = parse_numeric_value(row[spend_col]) or 0
                # Escludi anche righe con "Tutte le..." o simili
                if spend > 0 and name and "tutte le" not in name.lower():
                    top_ads.append((name[:40], spend))
            top_ads.sort(key=lambda x: x[1], reverse=True)
            
            if top_ads:
                output.append("   🏆 TOP 5 INSERZIONI PER SPESA:")
                for i, (name, spend) in enumerate(top_ads[:5], 1):
                    output.append(f"      {i}. {name:<40} €{format_number(spend)}")
                output.append("")
    
    # ========== TIKTOK CONTENT ==========
    elif file_type == "TIKTOK_CONTENT":
        views_col = next((c for c in df.columns if 'view' in c.lower() and 'total' in c.lower()), None)
        likes_col = next((c for c in df.columns if 'like' in c.lower() and 'total' in c.lower()), None)
        title_col = next((c for c in df.columns if 'title' in c.lower() or 'video title' in c.lower()), None)
        
        if views_col:
            views_list = []
            for _, row in df.iterrows():
                views = parse_numeric_value(row[views_col]) or 0
                title = str(row[title_col])[:50] if title_col else "—"
                likes = parse_numeric_value(row[likes_col]) or 0 if likes_col else 0
                if views > 0:
                    views_list.append((title, views, likes))
            
            if views_list:
                views_list.sort(key=lambda x: x[1], reverse=True)
                total_views = sum(v[1] for v in views_list)
                avg_views = total_views / len(views_list)
                
                output.append(f"🎬 CONTENUTI: {filename.split('/')[-1].replace('.csv', '')}")
                output.append("")
                output.append(f"   📊 Totale video: {len(views_list)}")
                output.append(f"   👁️ Visualizzazioni totali: {format_number(total_views)}")
                output.append(f"   📈 Media per video: {format_number(avg_views)}")
                output.append("")
                output.append("   🏆 TOP 5 VIDEO:")
                for i, (title, views, likes) in enumerate(views_list[:5], 1):
                    output.append(f"      {i}. {title[:45]}")
                    output.append(f"         👁️ {format_number(views):>10} | ❤️ {format_number(likes):>8}")
                output.append("")
    
    # ========== DEMOGRAPHICS ==========
    elif file_type == "DEMOGRAPHICS":
        # Cerca colonne genere
        uomini_col = next((c for c in df.columns if 'uomini' in c.lower()), None)
        donne_col = next((c for c in df.columns if 'donne' in c.lower()), None)
        age_col = next((c for c in df.columns if 'età' in c.lower() or 'age' in c.lower()), df.columns[0] if len(df.columns) > 0 else None)
        
        if uomini_col and donne_col:
            output.append(f"👥 DEMOGRAFIA: {filename.split('/')[-1].replace('.csv', '')}")
            output.append("")
            
            total_m = 0
            total_f = 0
            
            output.append("   👤 DISTRIBUZIONE PER ETÀ E GENERE:")
            for _, row in df.iterrows():
                age = str(row[age_col])[:15] if age_col else "—"
                m = parse_numeric_value(row[uomini_col]) or 0
                f = parse_numeric_value(row[donne_col]) or 0
                total_m += m
                total_f += f
                tot = m + f
                if tot > 0:
                    pct_m = (m / tot) * 100
                    pct_f = (f / tot) * 100
                    output.append(f"      {age:<15} | 👨 {pct_m:>5.1f}% | 👩 {pct_f:>5.1f}%")
            
            tot_gen = total_m + total_f
            if tot_gen > 0:
                output.append("")
                output.append(f"   📊 TOTALE: 👨 {total_m} ({total_m/tot_gen*100:.1f}%) | 👩 {total_f} ({total_f/tot_gen*100:.1f}%)")
            output.append("")
            
            # Città/Paesi se presenti
            geo_cols = [c for c in df.columns if any(x in c.lower() for x in ['città', 'citt', 'paesi', 'countr', 'territor'])]
            if geo_cols:
                output.append("   🌍 DISTRIBUZIONE GEOGRAFICA:")
                # Prendi prima riga con valori geografici
                for _, row in df.iterrows():
                    if geo_cols[0] in row and not pd.isna(row[geo_cols[0]]):
                        geo_val = str(row[geo_cols[0]])
                        if len(geo_val) > 3:  # Evita valori numerici
                            output.append(f"      • {geo_val}")
                output.append("")
    
    # ========== TIKTOK FOLLOWER ACTIVITY ==========
    elif file_type == "TIKTOK_FOLLOWER_ACTIVITY":
        date_col = next((c for c in df.columns if 'date' in c.lower()), None)
        hour_col = next((c for c in df.columns if 'hour' in c.lower()), None)
        active_col = next((c for c in df.columns if 'active' in c.lower() or 'follower' in c.lower()), None)
        
        if date_col and hour_col and active_col:
            # Calcola media per ora del giorno
            hour_stats = {}
            for _, row in df.iterrows():
                hour = str(row[hour_col])
                active = parse_numeric_value(row[active_col]) or 0
                if hour not in hour_stats:
                    hour_stats[hour] = []
                hour_stats[hour].append(active)
            
            if hour_stats:
                output.append(f"⏰ ATTIVITÀ FOLLOWER: {filename.split('/')[-1].replace('.csv', '')}")
                output.append("")
                output.append("   📊 MEDIA FOLLOWER ATTIVI PER ORA:")
                sorted_hours = sorted(hour_stats.items(), key=lambda x: sum(x[1])/len(x[1]), reverse=True)
                for hour, values in sorted_hours[:8]:  # Top 8 ore
                    avg = sum(values) / len(values)
                    output.append(f"      • Ore {hour:>2}:00 → {format_number(avg):>6} follower attivi (media)")
                output.append("")
                
                # Ora più attiva
                if sorted_hours:
                    best_hour, best_values = sorted_hours[0]
                    best_avg = sum(best_values) / len(best_values)
                    output.append(f"   ⭐ ORA PIÙ ATTIVA: {best_hour}:00 con {format_number(best_avg)} follower attivi in media")
                output.append("")
    
    # ========== TIKTOK FOLLOWER HISTORY ==========
    elif file_type == "TIKTOK_FOLLOWER_HISTORY":
        date_col = next((c for c in df.columns if 'date' in c.lower()), None)
        follower_col = next((c for c in df.columns if 'follower' in c.lower() and 'difference' not in c.lower()), None)
        diff_col = next((c for c in df.columns if 'difference' in c.lower()), None)
        
        if date_col and follower_col:
            followers = []
            dates = []
            diffs = []
            
            for _, row in df.iterrows():
                date_val = format_date(row[date_col])
                foll = parse_numeric_value(row[follower_col]) or 0
                diff = parse_numeric_value(row[diff_col]) or 0 if diff_col else 0
                if foll > 0:
                    followers.append(foll)
                    dates.append(date_val)
                    diffs.append(diff)
            
            if followers:
                output.append(f"📈 CRESCITA FOLLOWER: {filename.split('/')[-1].replace('.csv', '')}")
                output.append("")
                output.append(f"   Periodo: {dates[0] if dates else '—'} → {dates[-1] if dates else '—'}")
                output.append(f"   Follower iniziali: {format_number(followers[0])}")
                output.append(f"   Follower finali: {format_number(followers[-1])}")
                
                growth = followers[-1] - followers[0]
                growth_pct = (growth / followers[0] * 100) if followers[0] > 0 else 0
                output.append(f"   Crescita totale: {format_number(growth)} ({growth_pct:+.1f}%)")
                output.append("")
                
                # Giorni con più crescita
                if diffs:
                    positive_days = [(dates[i], diffs[i]) for i in range(len(diffs)) if diffs[i] > 0]
                    positive_days.sort(key=lambda x: x[1], reverse=True)
                    if positive_days:
                        output.append("   🚀 GIORNI CON PIÙ CRESCITA:")
                        for i, (date, diff) in enumerate(positive_days[:5], 1):
                            output.append(f"      {i}. {date:<12} → +{format_number(diff)} follower")
                        output.append("")
    
    # ========== TIKTOK OVERVIEW ==========
    elif file_type == "TIKTOK_OVERVIEW":
        date_col = next((c for c in df.columns if 'date' in c.lower()), None)
        views_col = next((c for c in df.columns if 'view' in c.lower() and 'video' in c.lower()), None)
        likes_col = next((c for c in df.columns if 'like' in c.lower()), None)
        comments_col = next((c for c in df.columns if 'comment' in c.lower()), None)
        shares_col = next((c for c in df.columns if 'share' in c.lower()), None)
        
        if date_col:
            total_views = 0
            total_likes = 0
            total_comments = 0
            total_shares = 0
            
            for _, row in df.iterrows():
                if views_col:
                    total_views += parse_numeric_value(row[views_col]) or 0
                if likes_col:
                    total_likes += parse_numeric_value(row[likes_col]) or 0
                if comments_col:
                    total_comments += parse_numeric_value(row[comments_col]) or 0
                if shares_col:
                    total_shares += parse_numeric_value(row[shares_col]) or 0
            
            output.append(f"📊 OVERVIEW TIKTOK: {filename.split('/')[-1].replace('.csv', '')}")
            output.append("")
            output.append(f"   Periodo analizzato: {len(df)} giorni")
            output.append("")
            output.append("   📈 TOTALE METRICHE:")
            output.append(f"      • Visualizzazioni video: {format_number(total_views)}")
            output.append(f"      • Like: {format_number(total_likes)}")
            output.append(f"      • Commenti: {format_number(total_comments)}")
            output.append(f"      • Condivisioni: {format_number(total_shares)}")
            if total_views > 0:
                engagement = ((total_likes + total_comments + total_shares) / total_views) * 100
                output.append(f"      • Engagement rate: {engagement:.2f}%")
            output.append("")
            
            # Media giornaliera
            days = len(df)
            if days > 0:
                output.append("   📅 MEDIA GIORNALIERA:")
                output.append(f"      • Visualizzazioni: {format_number(total_views/days)}")
                output.append(f"      • Like: {format_number(total_likes/days)}")
                output.append(f"      • Commenti: {format_number(total_comments/days)}")
            output.append("")
    
    # ========== TIKTOK VIEWERS ==========
    elif file_type == "TIKTOK_VIEWERS":
        date_col = next((c for c in df.columns if 'date' in c.lower()), None)
        total_col = next((c for c in df.columns if 'total' in c.lower() and 'viewer' in c.lower()), None)
        new_col = next((c for c in df.columns if 'new' in c.lower() and 'viewer' in c.lower()), None)
        return_col = next((c for c in df.columns if 'returning' in c.lower() and 'viewer' in c.lower()), None)
        
        if date_col and total_col:
            total_viewers = 0
            new_viewers = 0
            return_viewers = 0
            
            for _, row in df.iterrows():
                total_viewers += parse_numeric_value(row[total_col]) or 0
                if new_col:
                    new_viewers += parse_numeric_value(row[new_col]) or 0
                if return_col:
                    return_viewers += parse_numeric_value(row[return_col]) or 0
            
            output.append(f"👁️ VIEWERS TIKTOK: {filename.split('/')[-1].replace('.csv', '')}")
            output.append("")
            output.append(f"   Periodo: {len(df)} giorni")
            output.append("")
            output.append("   📊 TOTALE VIEWERS:")
            output.append(f"      • Viewers totali: {format_number(total_viewers)}")
            if new_viewers > 0:
                output.append(f"      • Nuovi viewers: {format_number(new_viewers)} ({new_viewers/total_viewers*100:.1f}%)")
            if return_viewers > 0:
                output.append(f"      • Viewers di ritorno: {format_number(return_viewers)} ({return_viewers/total_viewers*100:.1f}%)")
            output.append("")
    
    # ========== TIKTOK DEMOGRAPHICS ==========
    elif file_type == "TIKTOK_DEMOGRAPHICS":
        output.append(f"👥 DEMOGRAFIA TIKTOK: {filename.split('/')[-1].replace('.csv', '')}")
        output.append("")
        
        # Cerca colonne chiave
        gender_col = next((c for c in df.columns if 'gender' in c.lower()), None)
        distribution_col = next((c for c in df.columns if 'distribution' in c.lower() or 'percent' in c.lower()), None)
        territory_col = next((c for c in df.columns if 'territor' in c.lower() or 'countr' in c.lower()), None)
        
        if gender_col and distribution_col:
            output.append("   👤 DISTRIBUZIONE PER GENERE:")
            for _, row in df.iterrows():
                gender = str(row[gender_col])[:15]
                dist = parse_numeric_value(row[distribution_col]) or 0
                if dist < 1 and dist > 0:
                    dist = dist * 100  # Converti da decimale a percentuale
                if dist > 0:
                    output.append(f"      • {gender:<15} → {dist:>5.1f}%")
            output.append("")
        
        if territory_col:
            value_col = next((c for c in df.columns if c != territory_col and parse_numeric_value(df[c].iloc[0] if len(df) > 0 else None) is not None), None)
            if value_col:
                output.append("   🌍 TOP TERRITORI:")
                territories = []
                for _, row in df.iterrows():
                    terr = str(row[territory_col])[:30]
                    val = parse_numeric_value(row[value_col]) or 0
                    if val > 0:
                        territories.append((terr, val))
                territories.sort(key=lambda x: x[1], reverse=True)
                for i, (terr, val) in enumerate(territories[:10], 1):
                    output.append(f"      {i:>2}. {terr:<30} → {format_number(val):>10}")
                output.append("")
    
    # ========== FORMATTAZIONE GENERICA ==========
    else:
        output.append(f"📊 DATI: {filename.split('/')[-1].replace('.csv', '')}")
        output.append("")
        output.append(f"   Righe: {len(df)} | Colonne: {len(df.columns)}")
        output.append("")
        
        # Statistiche numeriche
        numeric_cols = {}
        for col in df.columns:
            values = []
            for v in df[col].dropna().head(100):
                num = parse_numeric_value(v)
                if num is not None:
                    values.append(num)
            if len(values) > 0:
                numeric_cols[col] = {
                    'total': sum(values),
                    'avg': sum(values) / len(values),
                    'max': max(values)
                }
        
        if numeric_cols:
            output.append("   📈 METRICHE PRINCIPALI:")
            for col, stats in list(numeric_cols.items())[:5]:
                output.append(f"      • {col[:35]:<35} | Tot: {format_number(stats['total']):>10} | Media: {format_number(stats['avg']):>10}")
            output.append("")
        
        # Anteprima
        output.append("   📋 ANTEPRIMA (prime 5 righe):")
        preview_cols = df.columns[:4] if len(df.columns) > 4 else df.columns
        for idx, row in df.head(5).iterrows():
            row_str = " | ".join([f"{str(row[col])[:15]:<15}" for col in preview_cols])
            output.append(f"      {row_str}")
        output.append("")
    
    output.append("=" * 80)
    output.append("✅ Report completato")
    
    return "\n".join(output)

# ============ MAIN APP ============

st.title("📄 CSV to Human-Readable Text Converter")
st.caption("Carica un file CSV e ottieni una versione leggibile in testo")

st.divider()

# Upload section
with st.expander("📂 Carica File CSV", expanded=True):
    if "uploader_key" not in st.session_state:
        st.session_state["uploader_key"] = str(uuid.uuid4())
    
    up_files = st.file_uploader(
        "Trascina qui i file CSV",
        accept_multiple_files=True,
        key=st.session_state["uploader_key"],
        type=['csv', 'txt']
    )
    
    if up_files:
        st.metric("File selezionati", len(up_files))
        
        if st.button("🔄 CONVERTI IN TESTO", type="primary", use_container_width=True):
            for file in up_files:
                st.markdown(f"### 📄 {file.name}")
                
                # Load CSV
                df, status = load_csv_simple(file)
                
                if df is None:
                    st.error(f"❌ Errore: {status}")
                else:
                    # Convert to text
                    readable_text = csv_to_readable_text(df, file.name)
                    
                    # Display
                    st.markdown('<div class="text-output">', unsafe_allow_html=True)
                    st.text(readable_text)
                    st.markdown('</div>', unsafe_allow_html=True)
                    
                    # Download button
                    st.download_button(
                        label="📥 Scarica come TXT",
                        data=readable_text,
                        file_name=f"{file.name.replace('.csv', '')}_readable.txt",
                        mime="text/plain"
                    )
                    
                    st.divider()

st.divider()

# Info section
with st.expander("ℹ️ Come funziona", expanded=False):
    st.markdown("""
    Questo convertitore:
    
    1. **Carica CSV** - Supporta vari encoding (UTF-8, Latin-1, etc.)
    2. **Rileva automaticamente** - Separatori, header, formato date
    3. **Formatta numeri** - Converte K/M in numeri leggibili
    4. **Formatta date** - Rende le date più comprensibili
    5. **Genera testo** - Crea un report leggibile
    
    **Formati supportati:**
    - Separatori: virgola, punto e virgola, tab
    - Encoding: UTF-8, UTF-16, Latin-1
    - Numeri: 1.234, 1K, 1.5M
    - Date: ISO, formato italiano, formato US
    """)
