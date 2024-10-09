import re

def extract_meaningful_text(markdown_content):
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

def main():
    with open('/data/users/brandon/ob1-projects/data_processing/md_completed/2024_10_09_cef6a159eb9fe11a20cdg.md', 'r', encoding='utf-8') as file:
        markdown_content = file.read()
    
    extracted_text = extract_meaningful_text(markdown_content)
    
    with open('extracted_text.txt', 'w', encoding='utf-8') as output_file:
        output_file.write(extracted_text)

if __name__ == "__main__":
    main()