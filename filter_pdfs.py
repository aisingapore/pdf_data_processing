import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType
from pypdf import PdfReader
import random
import csv

# Initialize Spark session
spark = SparkSession.builder.appName("PDF OCR Pipeline").getOrCreate()

# Define the path to the indo_journals directory
indo_journals_path = "/data/users/brandon/ob1-projects/data_processing/indo_journals"

# Step 1: List all PDF files
def list_pdf_files(directory: str) -> list[str]:
    """
    List all PDF files in the given directory.

    Args:
        directory (str): Path to the directory containing PDF files.

    Returns:
        list[str]: List of PDF file paths.
    """
    pdf_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(".pdf"):
                pdf_files.append(os.path.join(root, file))
    return pdf_files

all_pdf_files = list_pdf_files(indo_journals_path)

# Step 2: Function to get the number of pages in a PDF
def get_pdf_page_count(pdf_path: str) -> int:
    """
    Get the number of pages in a PDF file.

    Args:
        pdf_path (str): Path to the PDF file.

    Returns:
        int: Number of pages in the PDF.
    """
    try:
        with open(pdf_path, "rb") as file:
            pdf_reader = PdfReader(file)
            return len(pdf_reader.pages)
    except Exception:
        return 0

# Step 3: Sample PDFs with more than 2 pages
def sample_valid_pdfs(pdf_files: list[str], sample_size: int) -> list[str]:
    """
    Sample PDFs with more than 2 pages.

    Args:
        pdf_files (list[str]): List of PDF file paths.
        sample_size (int): Number of PDFs to sample.

    Returns:
        list[str]: List of sampled PDF file paths with more than 2 pages.
    """
    sampled_pdfs = []
    random.shuffle(pdf_files)
    
    for pdf in pdf_files:
        if get_pdf_page_count(pdf) > 2:
            sampled_pdfs.append(pdf)
            if len(sampled_pdfs) == sample_size:
                break
    
    return sampled_pdfs

sampled_pdfs = sample_valid_pdfs(all_pdf_files, 10)

# Define the destination directory for sampled PDFs
sampled_pdfs_dir = "/data/users/brandon/ob1-projects/data_processing/sampled_pdfs"

# New function to copy sampled PDFs to a directory
def copy_sampled_pdfs(sampled_pdfs: list[str], destination_dir: str) -> None:
    """
    Copy the sampled PDF files to a specified destination directory.

    Args:
        sampled_pdfs (list[str]): List of sampled PDF file paths.
        destination_dir (str): Path to the destination directory.

    Returns:
        None
    """
    os.makedirs(destination_dir, exist_ok=True)
    for pdf_path in sampled_pdfs:
        filename = os.path.basename(pdf_path)
        destination_path = os.path.join(destination_dir, filename)
        shutil.copy2(pdf_path, destination_path)
    print(f"Copied {len(sampled_pdfs)} PDFs to {destination_dir}")

# Copy the sampled PDFs to the destination directory
copy_sampled_pdfs(sampled_pdfs, sampled_pdfs_dir)

# Step 4: Perform OCR on sampled PDFs
def perform_ocr(pdf_path: str) -> str:
    """
    Perform OCR on a PDF file using PyPDF2.

    Args:
        pdf_path (str): Path to the PDF file.

    Returns:
        str: Extracted text from the PDF.
    """
    try:
        with open(pdf_path, "rb") as file:
            pdf_reader = PdfReader(file)
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text() + "\n"
        return text
    except Exception as e:
        return f"Error processing {pdf_path}: {str(e)}"

# Create UDFs for OCR and page count functions
ocr_udf = udf(perform_ocr, StringType())
page_count_udf = udf(get_pdf_page_count, IntegerType())

# Create a DataFrame from the sampled PDFs
pdf_df = spark.createDataFrame([(pdf,) for pdf in sampled_pdfs], ["pdf_path"])

# Apply the OCR function to each PDF and add page count
result_df = pdf_df.withColumn("page_count", page_count_udf(pdf_df["pdf_path"])) \
                  .withColumn("ocr_text", ocr_udf(pdf_df["pdf_path"]))

# Show the results
result_df.show(truncate=False)

# Step 5: Save the resulting DataFrame as a single CSV file
output_path = "/data/users/brandon/ob1-projects/data_processing/ocr_results.csv"

# Collect the results to the driver node
results = result_df.collect()

# Write the results to a single CSV file
row_count = 0
with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["pdf_path", "page_count", "ocr_text"])  # Write header
    for row in results:
        writer.writerow([row["pdf_path"], row["page_count"], row["ocr_text"]])
        row_count += 1

print(f"Results saved to: {output_path}")
print(f"Total number of rows in the CSV (including header): {row_count + 1}")

# Stop the Spark session
spark.stop()
