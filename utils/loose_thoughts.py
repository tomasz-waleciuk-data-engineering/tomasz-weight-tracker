import email
from email import policy
from email.parser import BytesParser
from bs4 import BeautifulSoup
import os
import re
from datetime import datetime, timedelta
from collections import defaultdict

# --- CONFIGURATION ---
FOLDER_PATH = r'/home/infot/git/pre_capstone_playground/utils/job_search_mhtml'

# Format: YYYY-MM-DD. 
CUTOFF_DATE_STR = '2025-12-02'

# Sort Direction: 'DESC' (Newest date first)
SORT_ORDER = 'DESC' 
# ---------------------

def process_folder(folder_path):
    if not os.path.exists(folder_path):
        print(f"Error: Folder not found at {folder_path}")
        return

    try:
        cutoff_date = datetime.strptime(CUTOFF_DATE_STR, "%Y-%m-%d")
        print(f"--- Config: Cutoff {CUTOFF_DATE_STR} | Output: Markdown (.md) ---")
    except ValueError:
        print("Error: Invalid CUTOFF_DATE_STR format.")
        return

    files = [f for f in os.listdir(folder_path) if f.lower().endswith('.mhtml')]
    files.sort()

    print(f"--- Scanning {len(files)} files... ---")

    all_collected_blocks = []
    unique_signatures = set()

    for filename in files:
        file_date_str = filename[:8]
        if not file_date_str.isdigit() or len(file_date_str) != 8:
            continue

        try:
            reference_date = datetime.strptime(file_date_str, "%Y%m%d")
        except ValueError:
            print(f"Skipping {filename}: Invalid date format.")
            continue

        full_path = os.path.join(folder_path, filename)
        parse_single_file(full_path, reference_date, file_date_str, cutoff_date, all_collected_blocks, unique_signatures)

    # --- GROUPING & SORTING ---
    print(f"--- Grouping {len(all_collected_blocks)} snapshots... ---")
    
    job_groups = defaultdict(list)
    for block in all_collected_blocks:
        if block['lines']:
            identifier = block['lines'][0] 
            job_groups[identifier].append(block)

    grouped_list = list(job_groups.values())
    reverse_sort = (SORT_ORDER.upper() == 'DESC')

    # Inner Sort
    for group in grouped_list:
        group.sort(key=lambda x: x['date_obj'], reverse=reverse_sort)

    # Outer Sort
    grouped_list.sort(key=lambda g: g[0]['date_obj'], reverse=reverse_sort)

    # --- GENERATE MARKDOWN OUTPUT ---
    final_output_lines = []
    
    final_output_lines.append(f"# Job Search Report")
    final_output_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}  ") # Note the double space
    final_output_lines.append(f"Total Jobs: {len(grouped_list)}  ")
    final_output_lines.append("") 
    
    for group in grouped_list:
        
        # 1. Job Header
        if group and group[0]['lines']:
            header_title = group[0]['lines'][0]
            # Add blank line before and after header for clean separation
            final_output_lines.append("")
            final_output_lines.append(f"### {header_title}")
            final_output_lines.append("")
        
        # 2. Loop through snapshots
        for i, block in enumerate(group):
            
            if i > 0:
                final_output_lines.append("")
                final_output_lines.append("* * *") 
                final_output_lines.append("")

            # Skip the first line (Header)
            lines_to_print = block['lines'][1:] if len(block['lines']) > 1 else []
            
            for line in lines_to_print:
                # IMPORTANT: We add "  " (two spaces) to the end of every string
                # This forces Markdown/PDF renders to create a hard line break.
                
                if " on 20" in line and any(x in line for x in ["Applied", "Successful", "Unsuccessful", "Rejected", "Viewed"]):
                    final_output_lines.append(f"**{line}**  ")
                
                elif line.startswith("Updated on "):
                    final_output_lines.append(f"_{line}_  ")
                
                elif "-  -  -" in line:
                    pass
                
                else:
                    final_output_lines.append(f"{line}  ")
        
        final_output_lines.append("")
        final_output_lines.append("---")

    # --- SAVE ---
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    output_filename = f"result-{timestamp}.md"
    output_full_path = os.path.join(folder_path, output_filename)

    try:
        with open(output_full_path, 'w', encoding='utf-8') as f:
            for line in final_output_lines:
                f.write(line + "\n")
        
        print(f"\n[SUCCESS] Markdown saved to: {output_full_path}")

    except Exception as e:
        print(f"\n[ERROR] Could not save file: {e}")


def parse_single_file(file_path, ref_date, date_str_label, cutoff_date, collection_list, seen_set):
    with open(file_path, 'rb') as fp:
        msg = BytesParser(policy=policy.default).parse(fp)

    for part in msg.walk():
        if part.get_content_type() == "text/html":
            try:
                payload_bytes = part.get_payload(decode=True)
                guessed_charset = part.get_content_charset()
                soup = BeautifulSoup(payload_bytes, 'html.parser', from_encoding=guessed_charset)

                for element in soup(["script", "style", "meta", "head", "title", "noscript"]):
                    element.decompose()

                text = soup.get_text(separator='\n')
                raw_lines = [line.strip() for line in text.splitlines()]
                clean_lines = [line for line in raw_lines if line]

                # --- Logic ---
                capture_mode = False
                start_marker = "Your recent activity"
                end_marker = "Show deleted jobs"
                current_block = []

                known_statuses = [
                    ("no longer considering", "No longer considering"),
                    ("unsuccessful", "Unsuccessful"),
                    ("successful", "Successful"),
                    ("rejected", "Rejected"),
                    ("viewed", "Viewed"),
                    ("applied", "Applied")
                ]

                for line in clean_lines:
                    if start_marker in line:
                        capture_mode = True
                        continue 
                    if end_marker in line:
                        capture_mode = False
                        break 

                    if capture_mode:
                        current_block.append(line)

                        if line.startswith("Updated on "):
                            
                            final_block_output = []
                            block_calculated_date = None 
                            
                            for i, block_line in enumerate(current_block):
                                
                                if block_line.strip() == "Update job":
                                    continue 
                                
                                match = re.search(r'(\d+)\s+day[^\s]*\s+ago', block_line, re.IGNORECASE)
                                
                                if match:
                                    days_ago = int(match.group(1))
                                    calculated_date = ref_date - timedelta(days=days_ago)
                                    formatted_date = calculated_date.strftime("%Y-%m-%d")
                                    block_calculated_date = calculated_date

                                    found_verb = None
                                    found_in_prev_line = False
                                    lower_line = block_line.lower()

                                    for keyword, label in known_statuses:
                                        if keyword in lower_line:
                                            found_verb = label
                                            break
                                    
                                    if not found_verb and i > 0:
                                        prev_line = current_block[i-1].lower()
                                        for keyword, label in known_statuses:
                                            if keyword in prev_line:
                                                found_verb = label
                                                found_in_prev_line = True
                                                break

                                    final_verb = found_verb if found_verb else "Applied"

                                    if found_in_prev_line:
                                        if final_block_output and final_block_output[-1] == current_block[i-1]:
                                            final_block_output.pop()

                                    final_block_output.append(f"{final_verb} on {formatted_date}")
                                else:
                                    final_block_output.append(block_line)

                            # --- STORE RESULT ---
                            if block_calculated_date and block_calculated_date >= cutoff_date:
                                block_signature = tuple(final_block_output)

                                if block_signature not in seen_set:
                                    seen_set.add(block_signature)
                                    
                                    job_entry = {
                                        'date_obj': block_calculated_date,
                                        'file_label': date_str_label,
                                        'lines': final_block_output
                                    }
                                    collection_list.append(job_entry)
                            
                            current_block = []

            except Exception as e:
                print(f"Error processing {file_path}: {e}")

if __name__ == "__main__":
    process_folder(FOLDER_PATH)