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
FOLDER_ID = '15RsQDnJLZTqmqmpQrUsJ-BDEODOmDm5k'  # <--- REMEMBER TO UPDATE THIS!
MASTER_CSV_NAME = "processed_weight_data_cache.csv"

try:
    import read_my_file as rmf
except ImportError:
    rmf = None

# ==========================================
# 2. PAGE CONFIG
# ==========================================
page_layout = st.sidebar.radio("Page layout:", options=['centered', 'wide'])
st.set_page_config(layout=page_layout, page_title="Weight Tracker")

# ==========================================
# 3. HELPER FUNCTIONS
# ==========================================

def parse_txt_content(content, file_id="unknown"):
    """Parses your specific text format."""
    rows = []
    lines = content.strip().split('\n')
    
    if len(lines) < 3: return []

    # Header: "Time:08:22, Fri,12/ 19/2025"
    header_line = lines[1]
    if "Time:" not in header_line: return []
        
    try:
        parts = header_line.split(',')
        time_str = parts[0].split('Time:')[1].strip()
        day_name = parts[1].strip()
        date_str = parts[2].strip()
    except:
        return []

    # Body: "Weight:90.4kg  ↑   Overweight"
    for line in lines[2:]:
        if ':' not in line: continue
        try:
            key_part, rest_part = line.split(':', 1)
            attribute = key_part.strip()
            tokens = re.split(r'\s+', rest_part.strip())
            
            raw_value = tokens[0]
            clean_value = raw_value.replace('kg','').replace('%','').replace('kcal','')
            info_symbol = tokens[1] if len(tokens) > 1 else ""
            info_txt = " ".join(tokens[2:]) if len(tokens) > 2 else ""
            
            row = [day_name, date_str, time_str, attribute, clean_value, info_symbol, info_txt, file_id]
            rows.append(row)
        except:
            continue
    return rows

@st.cache_resource
def get_drive_service():
    """Authenticates with Google Drive."""
    creds = None
    try:
        if "gcp_service_account" in st.secrets:
            creds = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
    except: pass

    if not creds and os.path.exists('credentials.json'):
        creds = service_account.Credentials.from_service_account_file('credentials.json')

    if not creds: return None
    return build('drive', 'v3', credentials=creds)

def download_file_content(service, file_id):
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done: _, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read().decode('utf-8')
    except: return ""

def get_master_cache(service, folder_id):
    q = f"'{folder_id}' in parents and name = '{MASTER_CSV_NAME}' and trashed=false"
    results = service.files().list(q=q, fields="files(id)").execute()
    files = results.get('files', [])
    if not files: return None, None
    
    csv_id = files[0]['id']
    content = download_file_content(service, csv_id)
    try:
        return pd.read_csv(io.StringIO(content)), csv_id
    except: return None, csv_id

def upload_master_cache(service, df, folder_id, existing_id=None):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    media_body = MediaIoBaseUpload(io.BytesIO(csv_buffer.getvalue().encode()), mimetype='text/csv')
    
    # ADDED EXPLICIT ERROR HANDLING HERE
    try:
        if existing_id:
            service.files().update(fileId=existing_id, media_body=media_body).execute()
        else:
            # Note: This usually fails for Service Accounts (Quota)
            meta = {'name': MASTER_CSV_NAME, 'parents': [folder_id]}
            service.files().create(body=meta, media_body=media_body).execute()
        return True
    except Exception as e:
        st.error(f"⚠️ UPLOAD FAILED: {e}")
        return False

@st.cache_data(ttl=3600, show_spinner=False)
def sync_drive_data(_service, folder_id):
    """Incremental Sync Logic."""
    master_df, master_id = get_master_cache(_service, folder_id)
    
    cols = ['day_name', 'date', 'time', 'attribute', 'value', 'info_symbol', 'info_txt', 'source_file_id']
    
    if master_df is None:
        master_df = pd.DataFrame(columns=cols)
    
    processed_ids = set(master_df['source_file_id'].unique()) if not master_df.empty else set()

    all_files_meta = []
    page_token = None
    while True:
        q = f"'{folder_id}' in parents and name contains '.txt' and trashed=false"
        res = _service.files().list(q=q, fields="nextPageToken, files(id, name)", pageToken=page_token).execute()
        all_files_meta.extend(res.get('files', []))
        page_token = res.get('nextPageToken')
        if not page_token: break
    
    new_files = [f for f in all_files_meta if f['id'] not in processed_ids]
    
    if new_files:
        new_rows = []
        status_text = st.empty()
        prog_bar = st.progress(0)
        
        for i, f in enumerate(new_files):
            if i % 10 == 0:
                status_text.text(f"Syncing new file {i+1}/{len(new_files)}...")
                prog_bar.progress((i+1)/len(new_files))
            
            content = download_file_content(_service, f['id'])
            if content:
                new_rows.extend(parse_txt_content(content, f['id']))
        
        prog_bar.empty()
        status_text.empty()
        
        if new_rows:
            new_df = pd.DataFrame(new_rows, columns=cols)
            full_df = pd.concat([master_df, new_df], ignore_index=True)
            
            # Try to upload
            status_text.text("Updating Cloud Cache (Saving to Google Drive)...")
            success = upload_master_cache(_service, full_df, folder_id, master_id)
            if success:
                st.toast("Cache Saved Successfully!", icon="✅")
            status_text.empty()
            
            return full_df
        else:
             st.warning("Downloaded files but found NO valid data rows. Parser might be mismatching.")
            
    return master_df

def bmi_to_kg_list(bmi_range, height):
    bmi_vs_kg = str(height) + ' cm:  '
    height /= 100
    for bmi in bmi_range:
        for dec in range(0,10,5):
            bmi_dec = bmi + dec/10
            bmi_vs_kg = ''.join([bmi_vs_kg, str(bmi_dec), ' = ', f'{bmi_dec * height**2:.1f}', ' kg, '])
    return bmi_vs_kg[:-2]

# ==========================================
# 4. MAIN APP LOGIC
# ==========================================

h1 = 182
h2 = h1 + 1
bmi_start = 25
bmi_end = 27

os_environ_hostname = os.environ.get('HOSTNAME', 'unknown-host')
st.write('\'HOSTNAME\' if known: ', os_environ_hostname)

# --- DATA LOADING SWITCH ---
is_cloud = "gcp_service_account" in st.secrets
df = pd.DataFrame()

if is_cloud:
    service = get_drive_service()
    if service:
        with st.spinner("Syncing Google Drive..."):
            df = sync_drive_data(service, FOLDER_ID)
    else:
        st.error("Cloud Auth Failed.")
else:
    # Local Logic
    drive = 'u:'
    path_a = 'OneDrive'
    path_b = 'DRIVE_GOOGLE'
    path_c = 'Adoric health'
    
    if os.environ.get('HOSTNAME') == 'streamlit':
        full_path = os.getcwd() + '/weight-checks-adoric-or-salter-mibody/Data'
    else:
        full_path = os.path.join(drive, '/', path_a, path_b, path_c)
        if not os.path.exists(full_path): full_path = './Data'

    if rmf:
        data_line_by_line, numer_of_files, _ = rmf.read_files_to_list(full_path)
        df = pd.DataFrame(data_line_by_line)
        df.columns = ['day_name', 'date', 'time', 'attribute', 'value', 'info_symbol', 'info_txt']

# ==========================================
# 5. DATA PRE-PROCESSING & VISUALS
# ==========================================

if not df.empty:
    df['date'] = df['date'].astype(str)
    df['time'] = df['time'].astype(str)
    
    df['date_time'] = pd.to_datetime(
        df['date'] + df['time'],
        format='mixed', 
        errors='coerce'
    )
    df.dropna(subset=['date_time'], inplace=True)

    pivoted_df = df.pivot_table(
        index='date_time', 
        columns='attribute', 
        values='value', 
        aggfunc='first'
    )

    if 'BMR' in pivoted_df.columns:
        pivoted_df.drop(columns='BMR', inplace=True)
        
    pivoted_df.sort_index(ascending=False, inplace=True)

    # --- YOUR ORIGINAL VISUALS ---
    date_when_diagnosed_with_diabetics_type_2 = '2025-05-12'
    days_since = (pd.Timestamp.today() - pd.to_datetime(date_when_diagnosed_with_diabetics_type_2)).days
    
    number_of_recent_readings = abs(days_since) if days_since != 0 else 83
        
    the_first_valid_entry = st.date_input("Remove entries before:", datetime(2022, 1, 1))

    fig_01_df = pivoted_df.iloc[:number_of_recent_readings].copy()
    fig_01_df = fig_01_df[fig_01_df.index >= str(the_first_valid_entry)]

    cols_to_numeric = ['Weight', 'BMI', 'Bone Mass', 'Muscle Mass', 'Body fat', 'Visceral fat', 'Body water']
    for c in cols_to_numeric:
        if c in fig_01_df.columns:
            fig_01_df[c] = pd.to_numeric(fig_01_df[c], errors='coerce')

    st.write(bmi_to_kg_list(range(bmi_start, bmi_end+1),h1))
    st.write(bmi_to_kg_list(range(bmi_start, bmi_end+1),h2))
    st.warning(f'All readings for {days_since} days')    
    
    if 'Weight' in fig_01_df.columns and 'BMI' in fig_01_df.columns:
        st.dataframe(fig_01_df[['Weight', 'BMI']])

        start_date = fig_01_df.index[0].date()
        max_weight = fig_01_df['Weight'].max()
        min_weight = fig_01_df['Weight'].min()
        trendline_window = '28D'

        fig_01 = px.scatter(
            fig_01_df,
            y='Weight',
            trendline='rolling',
            trendline_options=dict(function="mean", window=trendline_window),
            trendline_color_override="red",
            range_y=(min_weight-1, max_weight+1),
            hover_data=['Weight'],
        )
        fig_01.update_xaxes(
            showgrid=True, 
            gridwidth=1, 
            gridcolor='#dfdfdf', 
            tick0=(pd.Timestamp.today().date() - pd.Timedelta(number_of_recent_readings, 'D')),
            dtick=7*24*60*60*1000,
            tickangle=90,
        )
        fig_01.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#dfdfdf', dtick=1)

        week_day_today = datetime.today().strftime('%A')[:3]

        st.warning(f'Weight readings and a \'{trendline_window}\' trendline')
        st.plotly_chart(fig_01)

        st.warning('What is our preferred average calculations range?')
        frequency_for_agg = st.radio(
            f"Select mothly, weekly (week end Sun), weekly (week end Fri), (week end today: {week_day_today})",
            [f"W-{week_day_today}", "ME", "W-Sun", "W-Fri"],
            captions=[
                f"Weekly {week_day_today} (today)",
                "Monthly",
                "Weekly Sun",
                "Weekly Fri",
            ],
            horizontal=True,
        )

        weight_weekly_average_df = fig_01_df.drop(
            columns=['Bone Mass', 'Muscle Mass', 'Body fat', 'Visceral fat', 'Body water'], 
            errors='ignore'
        ).copy()

        weight_weekly_average_df = weight_weekly_average_df.resample(frequency_for_agg).mean().round(1)
        weight_weekly_average_df.sort_index(ascending=False, inplace=True)
        
        weight_weekly_average_df['weight_change'] = \
            weight_weekly_average_df['Weight'] - weight_weekly_average_df['Weight'].shift(-1)
        
        weight_weekly_average_df.reset_index(inplace=True)
        weight_weekly_average_df['date_time'] = pd.to_datetime(weight_weekly_average_df['date_time']).dt.date
        weight_weekly_average_df.set_index('date_time', inplace=True)
        weight_weekly_average_df.rename(
            columns={'BMI': 'average_bmi', 'Weight': 'average_weight'},
            inplace=True
        )

        st.dataframe(weight_weekly_average_df)

st.write(os.getcwd())
