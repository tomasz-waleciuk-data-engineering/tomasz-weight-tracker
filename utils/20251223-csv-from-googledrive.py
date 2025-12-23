import io
import os
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- 1. Configuration ---
# Replace this with your actual folder ID from the URL
FOLDER_ID = '15RsQDnJLZTqmqmpQrUsJ-BDEODOmDm5k' 

# Path to your local JSON key (for local testing)
LOCAL_CREDENTIALS_FILE = 'credentials.json'

# --- 2. Authentication Helper ---
def get_drive_service():
    """
    Authenticates using Streamlit Secrets (Production) 
    or a local JSON file (Development).
    """
    creds = None

    # 1. Attempt to load from Streamlit Secrets (Best for Cloud)
    try:
        # Accessing st.secrets will raise an error if secrets.toml is missing
        if "gcp_service_account" in st.secrets:
            creds_info = st.secrets["gcp_service_account"]
            creds = service_account.Credentials.from_service_account_info(creds_info)
    except Exception:
        # If secrets are missing, ignore the error and proceed to fallback
        pass
    
    # 2. Fallback: Attempt to load from Local JSON file (Best for Local Dev)
    if not creds and os.path.exists(LOCAL_CREDENTIALS_FILE):
        creds = service_account.Credentials.from_service_account_file(LOCAL_CREDENTIALS_FILE)
    
    # 3. If both failed
    if not creds:
        st.error(f"Authentication Error: No secrets found and '{LOCAL_CREDENTIALS_FILE}' is missing.")
        return None

    return build('drive', 'v3', credentials=creds)

# --- 3. Core Logic: Search & Pagination ---
def get_all_files_smart(service, parent_id):
    """
    Scans the folder and all subfolders.
    Handles Pagination (for >100 files) and Duplicates.
    Returns: Dictionary { 'Unique Display Name': 'File ID' }
    """
    unique_file_map = {}
    
    # Stack for recursion (start with the main folder)
    folders_to_search = [parent_id]
    
    while folders_to_search:
        current_folder_id = folders_to_search.pop()
        
        # Reset page token for the new folder we are scanning
        page_token = None

        # --- Pagination Loop (Keep asking until Google stops sending files) ---
        while True:
            # Query: Search inside current folder, not trash, for Folders OR .txt files
            query = (
                f"'{current_folder_id}' in parents and trashed=false and "
                f"(mimeType = 'application/vnd.google-apps.folder' or name contains '.txt')"
            )
            
            try:
                results = service.files().list(
                    q=query,
                    pageToken=page_token, # Send the token to get the next batch
                    fields="nextPageToken, files(id, name, mimeType, createdTime)",
                    pageSize=1000 # Maximize batch size for speed
                ).execute()
                
                items = results.get('files', [])
                
                for item in items:
                    # If it's a folder, add it to the stack to search later
                    if item['mimeType'] == 'application/vnd.google-apps.folder':
                        folders_to_search.append(item['id'])
                    
                    # If it's a file, add it to our list
                    else:
                        # Create Unique Label: Name | Date | ID Fragment
                        # Handle cases where createdTime might be missing
                        c_time = item.get('createdTime', 'Unknown')
                        date_short = c_time[:16].replace('T', ' ') if len(c_time) > 16 else c_time
                        id_short = item['id'][-4:]
                        
                        label = f"{item['name']} | {date_short} | ID:{id_short}"
                        unique_file_map[label] = item['id']
                
                # Check if there is another page of data
                page_token = results.get('nextPageToken')
                
                # If no token, we are done with this specific folder
                if not page_token:
                    break
                    
            except Exception as e:
                st.error(f"Error scanning folder ID {current_folder_id}: {e}")
                break

    return unique_file_map

# --- 4. Core Logic: Download Content ---
def get_file_content(service, file_id):
    """Downloads a file into memory and decodes it to string."""
    try:
        request = service.files().get_media(fileId=file_id)
        file_stream = io.BytesIO()
        downloader = MediaIoBaseDownload(file_stream, request)
        
        done = False
        while done is False:
            status, done = downloader.next_chunk()

        file_stream.seek(0)
        # Assuming standard UTF-8 text files
        return file_stream.read().decode('utf-8')
    except Exception as e:
        return f"Error reading file: {e}"

# --- 5. Main Streamlit UI ---
def main():
    st.title("Google Drive TXT Explorer")
    st.write("Recursive search with duplicate handling.")

    service = get_drive_service()
    
    if service:
        # Step 1: Scan Button
        if 'file_map' not in st.session_state:
            st.session_state['file_map'] = {}

        if st.button("Scan Google Drive Folder"):
            with st.spinner("Scanning folders... this might take a moment for 1000+ files..."):
                file_map = get_all_files_smart(service, FOLDER_ID)
                st.session_state['file_map'] = file_map
            
            if not file_map:
                st.warning("No .txt files found.")
            else:
                st.success(f"Scan Complete! Found {len(file_map)} files.")

        # Step 2: Display Results if we have them
        if st.session_state['file_map']:
            file_map = st.session_state['file_map']
            
            # Dropdown to select file
            selected_label = st.selectbox(
                f"Select a file ({len(file_map)} available):", 
                list(file_map.keys())
            )
            
            # Step 3: Read Button
            if st.button("Read Content"):
                file_id = file_map[selected_label]
                
                with st.spinner(f"Downloading {selected_label}..."):
                    content = get_file_content(service, file_id)
                    
                st.subheader("File Content:")
                st.text_area("Viewer", content, height=400)

if __name__ == "__main__":
    main()