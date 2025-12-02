"""
SOCIAL LOGIC - VERSIONE SEMPLIFICATA
Solo funzioni base per caricare CSV (usato dal convertitore)
"""

import pandas as pd
import io
import re
from datetime import datetime

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
