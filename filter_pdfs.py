import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pypdf import PdfReader
import random
import csv

# Initialize Spark session
spark = SparkSession.builder.appName("PDF OCR Pipeline").getOrCreate()

# Define the path to the indo_journals directory
indo_journals_path = "/data/users/brandon/ob1-projects/data_processing/indo_journals"

# Step 1: Randomly sample 100 PDF journals
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
sampled_pdfs = random.sample(all_pdf_files, min(10, len(all_pdf_files)))

# Step 2: Create a Spark DataFrame with OCR results
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

# Create a UDF (User Defined Function) for the OCR function
ocr_udf = udf(perform_ocr, StringType())

# Create a DataFrame from the sampled PDFs
pdf_df = spark.createDataFrame([(pdf,) for pdf in sampled_pdfs], ["pdf_path"])

# Apply the OCR function to each PDF
result_df = pdf_df.withColumn("ocr_text", ocr_udf(pdf_df["pdf_path"]))

# Show the results (you can modify this part based on your needs)
result_df.show(truncate=False)

# Step 3: Save the resulting DataFrame as a single CSV file
output_path = "/data/users/brandon/ob1-projects/data_processing/ocr_results.csv"

# Collect the results to the driver node
results = result_df.collect()

# Write the results to a single CSV file
with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["pdf_path", "ocr_text"])  # Write header
    for row in results:
        writer.writerow([row["pdf_path"], row["ocr_text"]])

print(f"Results saved to: {output_path}")

# Stop the Spark session
spark.stop()
