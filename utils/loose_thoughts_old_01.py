# def bmi_to_kg_list (bmi_range, height):
#     bmi_vs_kg = str(height) + ' cm:'
#     height /= 100
#     for bmi in bmi_range:
#         for dec in range(0,10,5):
#             bmi_dec = bmi + dec/10
#             bmi_vs_kg = ' '.join([bmi_vs_kg, '  ', str(bmi_dec), ':', f'{bmi_dec * height**2:.2f}'])
#     return bmi_vs_kg

# h1 = 182
# h2 = h1 + 1
# bmi_start = 25
# bmi_end = 28

# print(bmi_to_kg_list(range(bmi_start, bmi_end+1),h1))
# print(bmi_to_kg_list(range(bmi_start, bmi_end+1),h2))

#######################
#######################
# MHTML files parser! #
#######################
#######################

# import email
# from email import policy
# from email.parser import BytesParser

# my_file_path = '/mnt/t/_DOWNLOAD_/ROGcio/2025-12-03 A - Job applications - Universal Credit.mhtml'

# def parse_mhtml_archive(file_path):
#     print(f"--- Parsing MHTML: {file_path} ---")

#     with open(file_path, 'rb') as fp:
#         msg = BytesParser(policy=policy.default).parse(fp)

#     # MHTML is essentially a giant bucket of parts. We need to walk through them.
#     # The msg.walk() method iterates over every part of the MIME tree.
#     for part in msg.walk():
        
#         # 1. Get the Content Type (e.g., text/html, image/jpeg)
#         content_type = part.get_content_type()
        
#         # 2. Get the Content Location (The original URL of the resource)
#         # This is CRITICAL for MHTML. It tells us where this file lived on the web.
#         location = part.get('Content-Location')
        
#         if content_type == "text/html":
#             # This is usually the main page content
#             print(f"\n[Found HTML Page]")
#             print(f"Original URL: {location}")
            
#             # Extract the actual HTML content
#             # part.get_content() decodes the base64/quoted-printable automatically
#             try:
#                 html_content = part.get_content()
#                 print(f"Snippet: {html_content[:100].strip()}...")
#             except Exception as e:
#                 print(f"Could not decode HTML: {e}")

#         elif location:
#             # This is a resource (Image, CSS, Script)
#             print(f"[Found Resource] Type: {content_type} | URL: {location}")
            
#             # If you wanted to save the image, you would do:
#             # image_data = part.get_content()
#             # with open('saved_image.jpg', 'wb') as f: f.write(image_data)

# parse_mhtml_archive(my_file_path)

from email import policy
from email.parser import BytesParser
from bs4 import BeautifulSoup
import os

# Your specific file path (using 'r' for raw string to handle Windows backslashes)
FILE_PATH = r'/mnt/t/_DOWNLOAD_/ROGcio/2025-12-03 A - Job applications - Universal Credit.mhtml'

def parse_mhtml_filtered(file_path):
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return

    print(f"--- Opening: {file_path} ---")

    with open(file_path, 'rb') as fp:
        msg = BytesParser(policy=policy.default).parse(fp)

    found_html = False
    
    for part in msg.walk():
        if part.get_content_type() == "text/html":
            found_html = True
            try:
                # 1. Get raw content
                html_content = part.get_content()

                # 2. Clean with BeautifulSoup
                soup = BeautifulSoup(html_content, 'html.parser')

                # Remove invisible elements
                for element in soup(["script", "style", "meta", "head", "title", "noscript"]):
                    element.decompose()

                # Extract text
                text = soup.get_text(separator='\n')

                # 3. Split into a list of clean lines
                # We filter out purely empty lines immediately to make logic easier
                raw_lines = [line.strip() for line in text.splitlines()]
                clean_lines = [line for line in raw_lines if line]

                # 4. Apply Filtering Logic (Between X and Y)
                final_output = []
                capture_mode = False

                start_marker = "Your recent activity"
                end_marker = "Show deleted jobs"

                for line in clean_lines:
                    # Check for Start Marker
                    # We use 'in' to be safe, in case there is hidden whitespace or icons
                    if start_marker in line:
                        capture_mode = True
                        continue # Skip printing the marker itself

                    # Check for End Marker
                    if end_marker in line:
                        capture_mode = False
                        break # Stop processing completely

                    # Process the lines if we are in the "Active" zone
                    if capture_mode:
                        final_output.append(line)

                        # Check for the specific "Updated on" pattern
                        if line.startswith("Updated on "):
                            # Add the requested separator block
                            final_output.append("") # Empty line
                            final_output.append("########################################") 
                            final_output.append("") # Empty line

                # 5. Print Result
                print("--- FILTERED CONTENT ---")
                for line in final_output:
                    print(line)
                print("--- END FILTERED CONTENT ---")

            except Exception as e:
                print(f"Error processing HTML part: {e}")

    if not found_html:
        print("No 'text/html' content found.")

if __name__ == "__main__":
    parse_mhtml_filtered(FILE_PATH)