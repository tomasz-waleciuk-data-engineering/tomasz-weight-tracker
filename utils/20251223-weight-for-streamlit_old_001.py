# ==========================================
# CONFIGURATION
# ==========================================
# FOLDER_ID = '15RsQDnJLZTqmqmpQrUsJ-BDEODOmDm5k' # <--- PASTE YOUR FOLDER ID HERE

import os
import io
import re
import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# ==========================================
# 1. CONFIGURATION
# ==========================================
# REPLACE WITH YOUR ACTUAL FOLDER ID
FOLDER_ID = '15RsQDnJLZTqmqmpQrUsJ-BDEODOmDm5k' 
MASTER_CSV_NAME = "processed_weight_data_cache.csv"

# ==========================================
# 2. HELPER FUNCTIONS (PARSING & AUTH)
# ==========================================

def parse_txt_content(content, file_id="unknown"):
    """
    Parses the custom text format:
    Line 2: Time:08:22, Fri,12/ 19/2025
    Line 3+: Weight:90.4kg  ↑   Overweight
    """
    rows = []
    lines = content.strip().split('\n')
    
    if len(lines) < 3: return []

    # --- Header Parsing ---
    # Expected: "Time:08:22, Fri,12/ 19/2025"
    header_line = lines[1]
    if "Time:" not in header_line: return []
        
    try:
        parts = header_line.split(',')
        time_str = parts[0].split('Time:')[1].strip()
        day_name = parts[1].strip()
        date_str = parts[2].strip()
    except:
        return []

    # --- Body Parsing ---
    for line in lines[2:]:
        if ':' not in line: continue
        try:
            key_part, rest_part = line.split(':', 1)
            attribute = key_part.strip()
            
            # Regex split by whitespace
            tokens = re.split(r'\s+', rest_part.strip())
            
            raw_value = tokens[0]
            # Clean units
            clean_value = raw_value.replace('kg','').replace('%','').replace('kcal','')
            
            info_symbol = tokens[1] if len(tokens) > 1 else ""
            info_txt = " ".join(tokens[2:]) if len(tokens) > 2 else ""
            
            # Row structure: [Day, Date, Time, Attribute, Value, Symbol, Text, SourceID]
            row = [day_name, date_str, time_str, attribute, clean_value, info_symbol, info_txt, file_id]
            rows.append(row)
        except:
            continue
            
    return rows

@st.cache_resource
def get_drive_service():
    """Authenticates with Google Drive API."""
    creds = None
    # 1. Streamlit Secrets
    try:
        if "gcp_service_account" in st.secrets:
            creds = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
    except: pass

    # 2. Local File
    if not creds and os.path.exists('credentials.json'):
        creds = service_account.Credentials.from_service_account_file('credentials.json')

    if not creds: return None
    return build('drive', 'v3', credentials=creds)

def download_file_content(service, file_id):
    """Downloads a single file as string."""
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done: _, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read().decode('utf-8')
    except:
        return ""

# ==========================================
# 3. INCREMENTAL SYNC LOGIC
# ==========================================

def get_master_cache_data(service, folder_id):
    """Downloads the consolidated CSV if it exists."""
    q = f"'{folder_id}' in parents and name = '{MASTER_CSV_NAME}' and trashed=false"
    results = service.files().list(q=q, fields="files(id)").execute()
    files = results.get('files', [])
    
    if not files: return None, None
    
    csv_id = files[0]['id']
    content = download_file_content(service, csv_id)
    try:
        df = pd.read_csv(io.StringIO(content))
        return df, csv_id
    except:
        return None, csv_id

def upload_master_cache_data(service, df, folder_id, existing_id=None):
    """Uploads the updated dataframe back to Drive."""
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    media_body = MediaIoBaseUpload(io.BytesIO(csv_buffer.getvalue().encode()), mimetype='text/csv')

    if existing_id:
        service.files().update(fileId=existing_id, media_body=media_body).execute()
    else:
        meta = {'name': MASTER_CSV_NAME, 'parents': [folder_id]}
        service.files().create(body=meta, media_body=media_body).execute()

@st.cache_data(ttl=3600, show_spinner=False)
def sync_drive_data(_service, folder_id):
    """
    The Core Logic:
    1. Load Cache CSV
    2. Check for new files
    3. Download only new files
    4. Save updated Cache
    """
    # 1. Load Master Cache
    master_df, master_id = get_master_cache_data(_service, folder_id)
    if master_df is None:
        master_df = pd.DataFrame(columns=[
            'day_name', 'date', 'time', 'attribute', 'value', 
            'info_symbol', 'info_txt', 'source_file_id'
        ])
    
    processed_ids = set(master_df['source_file_id'].unique()) if not master_df.empty else set()

    # 2. Scan for ALL text files (Metadata only)
    all_files_meta = []
    page_token = None
    while True:
        q = f"'{folder_id}' in parents and name contains '.txt' and trashed=false"
        res = _service.files().list(q=q, fields="nextPageToken, files(id, name)", pageToken=page_token).execute()
        all_files_meta.extend(res.get('files', []))
        page_token = res.get('nextPageToken')
        if not page_token: break
    
    # 3. Identify New Files
    new_files = [f for f in all_files_meta if f['id'] not in processed_ids]
    
    if not new_files:
        return master_df # Nothing to do

    # 4. Download New Files
    new_rows = []
    status_text = st.empty()
    prog_bar = st.progress(0)
    
    for i, f in enumerate(new_files):
        if i % 10 == 0:
            status_text.text(f"Syncing new file {i+1}/{len(new_files)}...")
            prog_bar.progress((i+1)/len(new_files))
            
        content = download_file_content(_service, f['id'])
        if content:
            file_rows = parse_txt_content(content, f['id'])
            new_rows.extend(file_rows)
            
    prog_bar.empty()
    status_text.empty()

    # 5. Merge and Save
    if new_rows:
        new_df = pd.DataFrame(new_rows, columns=master_df.columns)
        full_df = pd.concat([master_df, new_df], ignore_index=True)
        
        # Determine if we should update cloud cache (Only if service acct has permission)
        try:
            status_text.text("Updating Cloud Cache...")
            upload_master_cache_data(_service, full_df, folder_id, master_id)
            status_text.empty()
        except Exception as e:
            st.warning(f"Could not update Cloud Cache (Check Permissions): {e}")
            
        return full_df
    
    return master_df

# ==========================================
# 4. MAIN APP LOGIC
# ==========================================

st.set_page_config(layout="wide", page_title="Weight Tracker")

# Detect Environment
is_cloud = "gcp_service_account" in st.secrets
data_loaded = False
df = pd.DataFrame()

if is_cloud:
    service = get_drive_service()
    if service:
        with st.spinner("Syncing with Google Drive..."):
            df = sync_drive_data(service, FOLDER_ID)
            data_loaded = True
    else:
        st.error("Authentication Failed.")
else:
    # Local Fallback (Mock data or local file logic)
    st.info("Local Mode detected. Please implement local file reading if needed.")

if data_loaded and not df.empty:
    # --- DATA PREPROCESSING ---
    # Ensure correct types for plotting
    df['date_time'] = pd.to_datetime(df['date'] + ' ' + df['time'], errors='coerce')
    df.dropna(subset=['date_time'], inplace=True)
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    
    # Pivot for Analysis
    pivoted_df = df.pivot_table(
        index='date_time', 
        columns='attribute', 
        values='value', 
        aggfunc='first' # Handle duplicate times if any
    )
    pivoted_df.sort_index(ascending=False, inplace=True)

    # --- UI & PLOTTING ---
    st.title("Weight Tracker")
    
    # Date Filtering
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", datetime(2022, 1, 1))
    
    filtered_df = pivoted_df[pivoted_df.index.date >= start_date]

    # Metrics
    if 'Weight' in filtered_df.columns:
        latest_weight = filtered_df['Weight'].iloc[0]
        prev_weight = filtered_df['Weight'].iloc[1] if len(filtered_df) > 1 else latest_weight
        delta = latest_weight - prev_weight
        
        st.metric("Latest Weight", f"{latest_weight} kg", f"{delta:.1f} kg", delta_color="inverse")
        
        # Plot
        fig = px.scatter(
            filtered_df, 
            y='Weight', 
            title="Weight History",
            trendline="rolling",
            trendline_options=dict(window=14), # 14-day moving average
            trendline_color_override="red"
        )
        st.plotly_chart(fig, use_container_width=True)

        # Data Table
        st.subheader("Recent Data")
        st.dataframe(filtered_df.head(10))
    else:
        st.warning("No 'Weight' data found in files.")

else:
    st.warning("No data found.")
    