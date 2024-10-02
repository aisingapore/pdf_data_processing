import os
import shutil
from typing import List, Optional
import PyPDF2
from tqdm import tqdm

def is_valid_pdf_with_content(file_path: str) -> bool:
    """
    Check if the file is a valid PDF with content.

    Args:
        file_path (str): The path to the PDF file.

    Returns:
        bool: True if the file is a valid PDF with content, False otherwise.
    """
    try:
        with open(file_path, "rb") as file:
            reader = PyPDF2.PdfReader(file)
            if len(reader.pages) == 0:
                return False
            # Check if at least one page has content
            for page in reader.pages:
                if page.extract_text().strip():
                    return True
        return False
    except PyPDF2.errors.PdfReadError:
        return False
    except Exception:
        return False

def get_folder_list(source_dir: str) -> List[str]:
    """
    Get a list of all folders in the source directory.

    Args:
        source_dir (str): The path to the source directory.

    Returns:
        List[str]: A list of folder paths.
    """
    folder_list = []
    for root, dirs, _ in os.walk(source_dir):
        folder_list.append(root)
        for dir in dirs:
            folder_list.append(os.path.join(root, dir))
    return folder_list

def collate_pdfs(source_dir: str, output_dir: str, max_files: Optional[int] = None) -> None:
    """
    Collate valid PDF files with content from the source directory and its subdirectories into the output directory.

    Args:
        source_dir (str): The path to the source directory containing PDFs.
        output_dir (str): The path to the output directory where PDFs will be copied.
        max_files (Optional[int]): Maximum number of files to copy. If None, copy all valid PDFs.

    Raises:
        FileNotFoundError: If the source directory doesn't exist.
        PermissionError: If there are permission issues accessing directories or files.
    """
    try:
        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)

        copied_files = 0
        folder_list = get_folder_list(source_dir)

        # Create a progress bar for folders
        with tqdm(total=len(folder_list), desc="Processing folders", unit="folder") as pbar:
            for folder in folder_list:
                if max_files is not None and copied_files >= max_files:
                    print(f"\nReached the maximum number of files ({max_files}). Stopping the process.")
                    break

                for file in os.listdir(folder):
                    if file.lower().endswith(".pdf"):
                        source_path = os.path.join(folder, file)
                        
                        # Check if the PDF is valid and has content
                        if is_valid_pdf_with_content(source_path):
                            dest_path = os.path.join(output_dir, file)
                            
                            # Handle duplicate filenames
                            counter = 1
                            while os.path.exists(dest_path):
                                name, ext = os.path.splitext(file)
                                dest_path = os.path.join(output_dir, f"{name}_{counter}{ext}")
                                counter += 1
                            
                            # Copy the PDF file
                            shutil.copy2(source_path, dest_path)
                            copied_files += 1
                            print(f"\nCopied ({copied_files}): {source_path} -> {dest_path}")

                            if max_files is not None and copied_files >= max_files:
                                break
                        else:
                            print(f"\nSkipped invalid or empty PDF: {source_path}")

                pbar.update(1)

        print(f"\nCopied {copied_files} files. Process complete.")

    except FileNotFoundError as e:
        print(f"Error: The source directory '{source_dir}' was not found.")
        raise e
    except PermissionError as e:
        print(f"Error: Permission denied when accessing files or directories.")
        raise e
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        raise e

def main() -> None:
    """
    Main function to execute the PDF collation process.
    """
    source_directory: str = input("Enter the source directory path: ").strip()
    output_directory: str = os.path.join(os.getcwd(), "indo_journals")

    copy_all = input("Do you want to copy all valid PDFs? (y/n): ").strip().lower()
    max_files = None
    if copy_all != "y":
        while True:
            try:
                max_files = int(input("Enter the maximum number of PDFs to copy: "))
                if max_files > 0:
                    break
                else:
                    print("Please enter a positive number.")
            except ValueError:
                print("Please enter a valid number.")

    try:
        collate_pdfs(source_directory, output_directory, max_files)
        print(f"PDF collation complete. Files copied to '{output_directory}'")
    except Exception as e:
        print(f"PDF collation failed: {str(e)}")

if __name__ == "__main__":
    main()