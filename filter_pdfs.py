import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, BooleanType
from pypdf import PdfReader
import json
import random
import csv
import anthropic  # Add this import
import re

# Initialize Spark session
spark = SparkSession.builder.appName("PDF OCR Pipeline").getOrCreate()

# Set up Anthropic client
anthropic_client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

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

def extract_output(text):
    pattern = r'<output>(.*?)</output>'
    match = re.search(pattern, text, re.DOTALL)
    return match.group(1).strip() if match else text

# Step 4: Perform OCR on sampled PDFs
def is_relevant_pdf(pdf_text: str) -> bool:
    """
    Use Anthropic LLM to determine if the PDF is a relevant academic paper in Indonesian.

    Args:
        pdf_text (str): Extracted text from the PDF.

    Returns:
        bool: True if the PDF is relevant, False otherwise.
    """
    client = anthropic.Anthropic(
        api_key=os.environ.get("ANTHROPIC_API_KEY")
    )

    prompt = f"""You are an expert Language Identification Detector. Your task is to determine if the primary content of a research paper is in Indonesian based on its OCR output. This task is crucial for training a Large Language Model to improve its Indonesian language performance.

Here is the OCR output of the research paper:

<ocr_output>
{pdf_text[:4000]}
</ocr_output>

Carefully analyze the OCR output and determine if the primary content is in Indonesian. Consider the following guidelines:

1. The majority of the text should be in Indonesian.
2. Technical terms, citations, or references in English are acceptable as long as they don't make up a significant portion of the content.
3. If the paper contains substantial sections in English (e.g., abstract, conclusion, or entire paragraphs) that are difficult to remove via REGEX scripts, consider the paper inappropriate for training the Indonesian LLM.
4. Be aware that OCR errors might affect some words, but focus on the overall language pattern.

If you encounter any English content that is more than just scattered words or phrases, and would be difficult to remove with simple REGEX scripts, consider the paper unsuitable for training the Indonesian LLM.

Provide your analysis and reasoning in the "reasoning" key of the JSON output. Then, based on your analysis, determine whether the paper is suitable (true) or unsuitable (false) for training the Indonesian LLM, and include this in the "answer" key of the JSON output.

Your output should be in the following JSON format:

<output>
{{
  "reasoning": "Your detailed analysis and reasoning here",
  "answer": true/false
}}
</output>

Remember to provide your reasoning before giving the final answer. Ensure your reasoning is thorough and supports your conclusion."""

    message = client.messages.create(
        model="claude-3-5-sonnet-20240620",
        max_tokens=1000,
        temperature=0,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ]
    )

    response_content = message.content[0].text
    output_content = extract_output(response_content)
    
    try:
        # Clean the output_content to remove any potential control characters
        cleaned_output = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', output_content)
        parsed_response = json.loads(cleaned_output)
        
        # Log the reasoning for debugging purposes
        print(f"LLM Reasoning: {parsed_response['reasoning']}")
        
        return parsed_response["answer"]
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        print(f"Raw output: {output_content}")
        
        # Fallback: If JSON parsing fails, check if the word "true" is in the response
        return "true" in output_content.lower()

def perform_ocr(pdf_path: str) -> str:
    """
    Perform OCR on a PDF file using pypdf

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

# Now define the sample_valid_pdfs function
def sample_valid_pdfs(pdf_files: list[str], sample_size: int) -> list[str]:
    """
    Sample PDFs with more than 2 pages and verified by Anthropic LLM.

    Args:
        pdf_files (list[str]): List of PDF file paths.
        sample_size (int): Number of PDFs to sample.

    Returns:
        list[str]: List of sampled PDF file paths that meet the criteria.
    """
    sampled_pdfs = []
    random.shuffle(pdf_files)
    
    for pdf in pdf_files:
        page_count = get_pdf_page_count(pdf)
        if 2 < page_count < 500:
            sampled_pdfs.append(pdf)
            if len(sampled_pdfs) == sample_size:
                break
    
    return sampled_pdfs

sampled_pdfs = sample_valid_pdfs(all_pdf_files, 5)

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


# Create UDFs for OCR and page count functions
ocr_udf = udf(perform_ocr, StringType())
page_count_udf = udf(get_pdf_page_count, IntegerType())

# Create a DataFrame from the sampled PDFs
pdf_df = spark.createDataFrame([(pdf,) for pdf in sampled_pdfs], ["pdf_path"])

print("Initial DataFrame schema:")
pdf_df.printSchema()

# Apply the OCR function, add page count
result_df = pdf_df.withColumn("page_count", page_count_udf(pdf_df["pdf_path"])) \
                  .withColumn("ocr_text", ocr_udf(pdf_df["pdf_path"])) \

print("\nDataFrame schema after adding page_count and ocr_text:")
result_df.printSchema()

is_relevant_udf = udf(is_relevant_pdf, BooleanType())

# Add LLM verification
result_df = result_df.withColumn("is_relevant", is_relevant_udf(result_df["ocr_text"]))

print("\nFinal DataFrame schema:")
result_df.printSchema()

# Step 5: Save the resulting DataFrame as a single CSV file
output_path = "/data/users/brandon/ob1-projects/data_processing/ocr_results.csv"

# Collect the results to the driver node
results = result_df.collect()

# Write the results to a single CSV file
row_count = 0
with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["pdf_path", "page_count", "is_relevant", "ocr_text"])  # Updated header
    for row in results:
        writer.writerow([row["pdf_path"], row["page_count"], row["is_relevant"], row["ocr_text"]])
        row_count += 1

print(f"Results saved to: {output_path}")
print(f"Total number of rows in the CSV (including header): {row_count + 1}")

# Stop the Spark session
spark.stop()
