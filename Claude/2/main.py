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

def csv_to_readable_text(df, filename=""):
    """Converte DataFrame in testo leggibile e sintetico"""
    
    if df.empty:
        return "⚠️ Il file CSV è vuoto o non contiene dati validi."
    
    output = []
    
    # Header sintetico
    output.append("╔" + "═" * 78 + "╗")
    output.append(f"║ {'📄 ' + filename:<76} ║")
    output.append("╠" + "═" * 78 + "╣")
    output.append(f"║ {'📊 Righe:':<20} {len(df):>56} ║")
    output.append(f"║ {'📋 Colonne:':<20} {len(df.columns):>56} ║")
    output.append("╚" + "═" * 78 + "╝")
    output.append("")
    
    # Riepilogo colonne (compatto)
    output.append("📌 COLONNE RILEVATE:")
    cols_per_line = 3
    for i in range(0, len(df.columns), cols_per_line):
        cols_chunk = df.columns[i:i+cols_per_line]
        cols_str = " | ".join([f"{j+1}. {col[:25]}" for j, col in enumerate(cols_chunk)])
        output.append(f"   {cols_str}")
    output.append("")
    
    # Analisi colonne numeriche (sintetica)
    numeric_stats = {}
    for col in df.columns:
        values = []
        for v in df[col].dropna().head(200):
            num = parse_numeric_value(v)
            if num is not None:
                values.append(num)
        
        if len(values) > 0:
            numeric_stats[col] = {
                'total': sum(values),
                'avg': sum(values) / len(values),
                'max': max(values),
                'min': min(values),
                'count': len(values)
            }
    
    if numeric_stats:
        output.append("📈 STATISTICHE PRINCIPALI:")
        output.append("")
        # Tabella compatta
        for col, stats in list(numeric_stats.items())[:8]:  # Max 8 colonne
            total_fmt = format_number(stats['total'])
            avg_fmt = format_number(stats['avg'])
            max_fmt = format_number(stats['max'])
            output.append(f"   {col[:30]:<30} │ Tot: {total_fmt:>12} │ Media: {avg_fmt:>10} │ Max: {max_fmt:>10}")
        output.append("")
    
    # Dati principali (sintetizzati)
    output.append("📋 ANTEPRIMA DATI:")
    output.append("")
    
    # Mostra solo prime 10 righe, in formato tabella compatta
    preview_rows = min(10, len(df))
    
    # Identifica colonne chiave (date, numeri importanti, testo)
    key_cols = []
    for col in df.columns:
        col_lower = col.lower()
        if any(x in col_lower for x in ['date', 'data', 'time', 'giorno']):
            key_cols.insert(0, col)  # Date prima
        elif any(x in col_lower for x in ['view', 'like', 'follower', 'reach', 'spend', 'impression', 'total']):
            if col not in key_cols:
                key_cols.append(col)
        elif len(key_cols) < 4:  # Max 4 colonne chiave
            key_cols.append(col)
    
    # Se ci sono troppe colonne, mostra solo le prime 5
    display_cols = key_cols[:5] if len(df.columns) > 5 else df.columns
    
    # Header tabella
    header_line = "   " + " │ ".join([f"{col[:18]:<18}" for col in display_cols])
    output.append(header_line)
    output.append("   " + "─" * (len(header_line) - 3))
    
    # Righe dati
    for idx, row in df.head(preview_rows).iterrows():
        row_data = []
        for col in display_cols:
            value = row[col]
            if pd.isna(value) or str(value).strip() == '':
                formatted = "—"
            elif any(x in col.lower() for x in ['date', 'data', 'time', 'giorno']):
                formatted = format_date(value)[:18]
            elif any(x in col.lower() for x in ['view', 'like', 'follower', 'reach', 'spend', 'impression']):
                formatted = format_number(value)
            else:
                formatted = str(value)[:18]
            row_data.append(f"{formatted:<18}")
        
        output.append("   " + " │ ".join(row_data))
    
    if len(df) > preview_rows:
        output.append(f"   ... e altre {len(df) - preview_rows} righe")
    
    output.append("")
    output.append("─" * 80)
    output.append("✅ Report generato con successo")
    
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
