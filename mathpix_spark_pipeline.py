import csv
import os
import json
import requests
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import re
import urllib.parse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def submit_pdf_request(file_path, options):
    """Submit a PDF processing request to Mathpix API."""
    # Remove the 'file:' prefix if present
    if file_path.startswith("file:"):
        file_path = urllib.parse.unquote(file_path[5:])
    
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

def process_pdf(pdf_path):
    """Process a PDF file using Mathpix API and return the MD content."""
    options = {
        "conversion_formats": {"md": True},
        "rm_spaces": True
    }

    initial_response = submit_pdf_request(pdf_path, options)
    if "pdf_id" not in initial_response:
        return f"Error: No pdf_id in the response for {pdf_path}"

    pdf_id = initial_response["pdf_id"]

    while True:
        status = check_pdf_status(pdf_id)
        if status.get("status") == "completed":
            headers = {
                "app_id": "aisingapore_cab6cc_cbdab1",
                "app_key": os.environ.get("MATHPIX_API_KEY")
            }
            url = f"https://api.mathpix.com/v3/pdf/{pdf_id}.md"
            response = requests.get(url, headers=headers)
            return response.text
        elif status.get("status") == "error":
            return f"Error in processing PDF: {json.dumps(status)}"
        else:
            time.sleep(2)

def extract_meaningful_text(markdown_content):
    """Extract meaningful text from markdown content."""
    # Remove metadata and formatting
    content = re.sub(r'^---.*?---', '', markdown_content, flags=re.DOTALL)
    
    # Remove title tags but keep the title text
    content = re.sub(r'^#\s*(.*?)\n', r'\1\n', content)
    
    # Remove #### symbols but keep the header content
    content = re.sub(r'^####\s*(.*?)\n', r'\1\n', content, flags=re.MULTILINE)
    content = re.sub(r'^##\s*(.*?)\n', r'\1\n', content, flags=re.MULTILINE)
    
    content = re.sub(r'\*\*.*?\*\*', '', content)
    content = re.sub(r'\$.*?\$', '', content)
    
    # Remove citations and references
    content = re.sub(r'\[.*?\]', '', content)
    content = re.sub(r'\(.*?\)', '', content)
    
    # Remove figure captions and table content
    content = re.sub(r'Gambar \d+\..*', '', content)
    
    # Remove Markdown tables (improved version)
    content = re.sub(r'\|[^\n]*\|(\n\|[-:| ]+\|)?(\n\|[^\n]*\|)*', '', content)
    
    # Remove Keywords section
    content = re.sub(r'Keywords:.*?(?=\n\n)', '', content, flags=re.DOTALL)
    
    # Remove extra whitespace and newlines
    content = re.sub(r'\s+', ' ', content)
    content = content.strip()
    
    return content

# Create Spark session
spark = SparkSession.builder.appName("MathpixPDFProcessing").getOrCreate()

# Define UDFs
process_pdf_udf = udf(process_pdf, StringType())
extract_meaningful_text_udf = udf(extract_meaningful_text, StringType())

# Load CSV file
df = spark.read.csv("/data/users/brandon/ob1-projects/data_processing/sample_filtered_5_true.csv", header=True, inferSchema=True)

# Filter rows where is_relevant is True
filtered_df = df.filter(col("is_relevant") == True)

# Step 1: Apply the process_pdf_udf
logging.info("Starting Step 1: process_pdf_udf")
result_df_step1 = filtered_df.withColumn("md_extraction_result", process_pdf_udf(col("pdf_path")))
result_df_step1.cache()
count_step1 = result_df_step1.count()
logging.info(f"Completed Step 1. Row count: {count_step1}")

# Step 2: Apply the extract_meaningful_text_udf
logging.info("Starting Step 2: extract_meaningful_text_udf")
result_df_step2 = result_df_step1.withColumn("extracted_meaningful_text", extract_meaningful_text_udf(col("md_extraction_result")))
count_step2 = result_df_step2.count()
logging.info(f"Completed Step 2. Row count: {count_step2}")

# Check the Spark UI for stage information
print(f"Check the Spark UI at: {spark.sparkContext.uiWebUrl}")

# Write the result to a single CSV file
# result.coalesce(1).write.csv("/data/users/brandon/ob1-projects/data_processing/sample_5_final_output.csv", header=True, 
# mode="overwrite")

# Collect the results to the driver node
results = result_df_step2.collect()

# Write the results to a single CSV file
row_count = 0
with open("/data/users/brandon/ob1-projects/data_processing/sample_5_final_output.csv", "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["pdf_path", "page_count", "md_extraction_result", "extracted_meaningful_text"])  # Write header
    for row in results:
        writer.writerow([row["pdf_path"], row["page_count"], row["md_extraction_result"], row["extracted_meaningful_text"]])
        row_count += 1

print(f"Results saved to: /data/users/brandon/ob1-projects/data_processing/sample_5_final_output.csv")
print(f"Total number of rows in the CSV (including header): {row_count + 1}")

# Stop the Spark session
spark.stop()