import json
import os
from wsgiref import headers
import requests
import time

def submit_pdf_request(file_path, options):
    """Submit a PDF processing request to Mathpix API."""
    r = requests.post(
        "https://api.mathpix.com/v3/pdf",
        headers={
            "app_id": "aisingapore_cab6cc_cbdab1",
            "app_key": os.environ.get("MATHPIX_API_KEY")
        },
        data={
            "options_json": json.dumps(options)
        },
        files={
            "file": open(file_path, "rb")
        }
    )
    return r.json()

def check_pdf_status(pdf_id):
    """Check the status of a PDF processing request."""
    r = requests.get(
        f"https://api.mathpix.com/v3/pdf/{pdf_id}",
        headers={
            "app_id": "aisingapore_cab6cc_cbdab1",
            "app_key": os.environ.get("MATHPIX_API_KEY")
        }
    )
    return r.json()

# Define options
# options = {
#     "conversion_formats": {"md": True}  # Request Markdown output
    # "math_inline_delimiters": ["", ""],  # Remove math delimiters
    # "math_display_delimiters": ["", ""],  # Remove display math delimiters
    # "rm_fonts": True,  # Remove font information
    # "rm_spaces": True,  # Remove extra spaces
    # "enable_tables": False,  # Disable table recognition
    # "enable_charts": False,  # Disable chart recognition
    # "enable_figures": False,  # Disable figure recognition
    # "enable_math_ocr": False,  # Disable math OCR
    # "enable_chemistry_ocr": False,  # Disable chemistry OCR
    # "enable_markdown_math": False,  # Disable Markdown math
    # "enable_latex_ocr": False,  # Disable LaTeX OCR
    # "alphabets_allowed": {
    #     "formats": ["text"],
    #     "alphabets_allowed": {
    #         "en": True,  # Allow English text
    #         # Add other languages if needed
    #     }
    # },
# }

options = {
    "conversion_formats": {"md": True},
    "rm_spaces": True
}

# File path
file_path = "/data/users/brandon/ob1-projects/data_processing/indo_journals/8495-Article Text-23908-1-10-20140915.pdf" # TODO: fix pdf titles containing spaces, add a - or something

# Submit the request
initial_response = submit_pdf_request(file_path, options)
print("Initial response:", json.dumps(initial_response, indent=2))

if "pdf_id" not in initial_response:
    print("Error: No pdf_id in the response")
    exit(1)

pdf_id = initial_response["pdf_id"]

# Poll for results
while True:
    status = check_pdf_status(pdf_id)
    print("Current status:", json.dumps(status, indent=2))
    
    if status.get("status") == "completed":
        print("PDF processing completed:")
        print(json.dumps(status, indent=2))
        headers = {
            "app_id": "aisingapore_cab6cc_cbdab1",
            "app_key": os.environ.get("MATHPIX_API_KEY")
        }
        url = "https://api.mathpix.com/v3/pdf/" + pdf_id + ".md"
        response = requests.get(url, headers=headers)
        # write to MD file in md_completed folder
        with open("md_completed/" + pdf_id + ".md", "w") as f:
            f.write(response.text)

        break
    elif status.get("status") == "error":
        print("Error in processing PDF:")
        print(json.dumps(status, indent=2))
        break
    else:
        print("Processing... Waiting 10 seconds before checking again.")
        time.sleep(10)