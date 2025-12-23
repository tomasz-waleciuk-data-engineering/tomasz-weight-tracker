import os
import io
import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- Try to import your local module, but don't crash if it's missing on cloud ---
try:
    import read_my_file as rmf
except ImportError:
    rmf = None

# ==========================================
# CONFIGURATION
# ==========================================
FOLDER_ID = '15RsQDnJLZTqmqmpQrUsJ-BDEODOmDm5k' # <--- PASTE YOUR FOLDER ID HERE

# ==========================================
# GOOGLE DRIVE HELPER FUNCTIONS
# ==========================================
def get_drive_service():
    """Authenticates using Streamlit Secrets or Local JSON"""
    creds = None
    # 1. Try Streamlit Secrets (Cloud)
    try:
        if "gcp_service_account" in st.secrets:
            creds = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
    except:
        pass

    # 2. Fallback to local JSON (Local)
    if not creds and os.path.exists('credentials.json'):
        creds = service_account.Credentials.from_service_account_file('credentials.json')

    if not creds: return None
    return build('drive', 'v3', credentials=creds)

def get_all_files_content(service, folder_id):
    """
    Downloads ALL files from the folder (and subfolders)
    and returns a list of lists: [[col1, col2...], [col1, col2...]]
    """
    all_data_rows = []
    folders_to_search = [folder_id]
    files_processed_count = 0

    while folders_to_search:
        current_folder = folders_to_search.pop()
        page_token = None
        
        while True:
            q = f"'{current_folder}' in parents and trashed=false"
            results = service.files().list(
                q=q, 
                pageToken=page_token,
                fields="nextPageToken, files(id, name, mimeType)", 
                pageSize=1000
            ).execute()
            
            for item in results.get('files', []):
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    folders_to_search.append(item['id'])
                else:
                    # It is a file: Download it
                    files_processed_count += 1
                    try:
                        request = service.files().get_media(fileId=item['id'])
                        fh = io.BytesIO()
                        downloader = MediaIoBaseDownload(fh, request)
                        done = False
                        while not done:
                            _, done = downloader.next_chunk()
                        
                        fh.seek(0)
                        content = fh.read().decode('utf-8')
                        
                        # --- PARSING LOGIC ---
                        # This mimics what your 'read_my_file' module likely does.
                        # We split by new line, then split by comma (or whatever separator you use)
                        lines = content.split('\n')
                        for line in lines:
                            if line.strip(): # Skip empty lines
                                # IMPORTANT: Assuming your file is Comma Separated. 
                                # If it is Tab separated, change to line.split('\t')
                                parts = line.split(',') 
                                
                                # Clean up whitespace around items
                                parts = [p.strip() for p in parts]
                                
                                # Ensure we have enough columns (your DF expects 7)
                                if len(parts) >= 7:
                                    all_data_rows.append(parts[:7]) 
                    except Exception as e:
                        print(f"Error reading {item['name']}: {e}")

            page_token = results.get('nextPageToken')
            if not page_token: break
            
    return all_data_rows, files_processed_count

# ==========================================
# MAIN APP LOGIC
# ==========================================
def bmi_to_kg_list (bmi_range, height):
    bmi_vs_kg = str(height) + ' cm:  '
    height /= 100
    for bmi in bmi_range:
        for dec in range(0,10,5):
            bmi_dec = bmi + dec/10
            bmi_vs_kg = ''.join([bmi_vs_kg, str(bmi_dec), ' = ', f'{bmi_dec * height**2:.1f}', ' kg, '])
    return bmi_vs_kg[:-2]

h1 = 182
h2 = h1 + 1
bmi_start = 25
bmi_end = 27

page_layout = st.sidebar.radio("Page layout:", options=['centered', 'wide'])
st.set_page_config(layout=page_layout)

st.write('Host Environment:', os.environ.get('HOSTNAME', 'Local/Cloud'))

# --- DATA LOADING SWITCH ---
data_line_by_line = []
numer_of_files = 0

# Check if we have secrets (Cloud) or if we are local
is_cloud_env = "gcp_service_account" in st.secrets

if is_cloud_env:
    # --- GOOGLE DRIVE PATH ---
    st.info("☁️ Running in Cloud Mode - Fetching from Google Drive...")
    service = get_drive_service()
    
    if service:
        with st.spinner("Connecting to Google Drive and downloading files..."):
            data_line_by_line, numer_of_files = get_all_files_content(service, FOLDER_ID)
    else:
        st.error("Authentication failed. Check Streamlit Secrets.")

else:
    # --- LOCAL DISK PATH ---
    st.info("💻 Running in Local Mode - Fetching from Disk...")
    
    # Your original path logic
    drive = 'u:'
    path_a = 'OneDrive'
    path_b = 'DRIVE_GOOGLE'
    path_c = 'Adoric health'
    # Try to construct path, fallback to current dir if drive doesn't exist
    try:
        full_path = os.path.join(drive, '/', path_a, path_b, path_c)
        if not os.path.exists(full_path): raise FileNotFoundError
    except:
        full_path = './Data' # Fallback for testing
        
    if rmf:
        data_line_by_line, numer_of_files, _ = rmf.read_files_to_list(full_path)
    else:
        st.error("Module 'read_my_file' not found and not in Cloud mode.")

st.write(f'Number of processed files: {numer_of_files}')

# --- PROCEED WITH EXISTING LOGIC ---
if data_line_by_line:
    df = pd.DataFrame(data_line_by_line)
    
    # Ensure we have 7 columns. If data has 6 or 8, this might error.
    # We take the first 7 columns just in case
    df = df.iloc[:, :7] 
    
    df.columns = [
        'day_name', 'date', 'time', 'attribute', 'value', 'info_symbol', 'info_txt'
    ]
    
    # Standardize types
    df['date'] = df['date'].astype(str)
    df['time'] = df['time'].astype(str)
    
    df['date_time'] = pd.to_datetime(
        df['date'] + df['time'],
        format='mixed', # 'mixed' is safer if formats vary
        errors='coerce' # Ignore errors if bad data exists
    )
    
    # Drop rows where date failed parsing
    df = df.dropna(subset=['date_time'])

    pivoted_df = df.pivot(index='date_time', columns='attribute', values='value')
    
    # Clean up columns if they exist
    if 'BMR' in pivoted_df.columns:
        pivoted_df.drop(columns='BMR', inplace=True)
        
    pivoted_df.sort_index(ascending=False, inplace=True)

    # ... REST OF YOUR PLOTTING CODE ...
    # (I have preserved your plotting logic below)

    date_when_diagnosed_with_diabetics_type_2 = '2025-05-12'
    # Fixed the future date issue for calculation purposes
    target_date = pd.to_datetime(date_when_diagnosed_with_diabetics_type_2)
    today = pd.Timestamp.today()
    
    # If date is in future, days is negative. Just showing absolute or handling it.
    days_diff = (today - target_date).days
    
    number_of_recent_readings = 83 # Default
    
    try:
        user_input = st.text_input(
            f'Provide integer number of recent records (Days from {date_when_diagnosed_with_diabetics_type_2}: {days_diff})',
            str(abs(days_diff)) if days_diff != 0 else "83"
        )
        number_of_recent_readings = int(user_input)
    except ValueError:
        pass

    the_first_valid_entry = st.date_input("Remove entries before:", datetime(2022, 1, 1))

    fig_01_df = pivoted_df.iloc[:number_of_recent_readings].copy()
    fig_01_df = fig_01_df[fig_01_df.index >= str(the_first_valid_entry)]

    # Ensure numeric
    cols_to_numeric = ['Weight', 'BMI', 'Bone Mass', 'Muscle Mass', 'Body fat', 'Visceral fat', 'Body water']
    for c in cols_to_numeric:
        if c in fig_01_df.columns:
            fig_01_df[c] = pd.to_numeric(fig_01_df[c], errors='coerce')

    st.write(bmi_to_kg_list(range(bmi_start, bmi_end+1),h1))
    st.write(bmi_to_kg_list(range(bmi_start, bmi_end+1),h2))
    
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
            # Add safety check in case min/max are NaN
            range_y=(min_weight-1 if pd.notnull(min_weight) else 0, 
                     max_weight+1 if pd.notnull(max_weight) else 100),
            hover_data=['Weight'],
        )
        st.plotly_chart(fig_01)
        
        # --- Aggregation Logic ---
        week_day_today = datetime.today().strftime('%A')[:3]
        frequency_for_agg = st.radio(
            "Aggregation Frequency",
            [f"W-{week_day_today}", "ME", "W-Sun", "W-Fri"],
            horizontal=True,
        )

        # Select only numeric columns for resampling to avoid errors
        numeric_cols = fig_01_df.select_dtypes(include=['float64', 'int64']).columns
        weight_weekly_average_df = fig_01_df[numeric_cols].resample(frequency_for_agg).mean().round(1)
        
        weight_weekly_average_df.sort_index(ascending=False, inplace=True)
        
        if 'Weight' in weight_weekly_average_df.columns:
            weight_weekly_average_df['weight_change'] = \
                weight_weekly_average_df['Weight'] - weight_weekly_average_df['Weight'].shift(-1)
        
        st.dataframe(weight_weekly_average_df)
    else:
        st.warning("Weight or BMI columns missing from data.")
else:
    st.warning("No data found.")