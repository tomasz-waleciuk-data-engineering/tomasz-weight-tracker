# --- CONFIGURATION ---
# FOLDER_PATH = r'/home/infot/git/pre_capstone_playground/utils/TD2_TESTY_MHTML'

import os
import email
from email import policy
from email.parser import BytesParser
from bs4 import BeautifulSoup

def parse_mhtml_tables_utf8(folder_path):
    if not os.path.exists(folder_path):
        print(f"Error: The folder '{folder_path}' does not exist.")
        return

    for filename in os.listdir(folder_path):
        if filename.lower().endswith(('.mhtml', '.mht')):
            file_path = os.path.join(folder_path, filename)
            
            print(f"\n{'='*40}")
            print(f"FILE: {filename}")
            print(f"{'='*40}")

            try:
                with open(file_path, 'rb') as f:
                    msg = BytesParser(policy=policy.default).parse(f)

                for part in msg.walk():
                    if part.get_content_type() == "text/html":
                        # --- THE FIX IS HERE ---
                        # 1. Get raw binary bytes instead of a decoded string.
                        payload_bytes = part.get_payload(decode=True)
                        
                        # 2. Get the charset declared in the email header (e.g. "windows-1252" or "utf-8")
                        declared_charset = part.get_content_charset()

                        # 3. Pass bytes + declared charset to BeautifulSoup.
                        # BS4 will try the declared charset first. If that fails or is missing,
                        # it analyzes the HTML bytes to guess the correct encoding automatically.
                        soup = BeautifulSoup(payload_bytes, "html.parser", from_encoding=declared_charset)

                        # --- Table Extraction Logic ---
                        tables = soup.find_all("table")
                        
                        if not tables:
                            print("No <table> tags found.")
                            continue

                        for i, table in enumerate(tables):
                            print(f"\n--- Table {i+1} ---")
                            rows = table.find_all("tr")
                            
                            for row in rows:
                                cells = row.find_all(["td", "th"])
                                
                                # get_text(strip=True) handles spacing. 
                                # We also replace non-breaking spaces (\xa0) with normal spaces to be safe.
                                row_data = [cell.get_text(strip=True).replace('\xa0', ' ') for cell in cells]
                                
                                if any(row_data):
                                    print(" | ".join(row_data))

            except Exception as e:
                print(f"Error parsing {filename}: {e}")

# --- USAGE ---
# Update this path
folder_location = r'/home/infot/git/pre_capstone_playground/utils/TD2_TESTY_MHTML'

parse_mhtml_tables_utf8(folder_location)
