import re

def remove_braced_text(text):
    # Remove all text within curly braces, including the braces themselves
    return re.sub(r'\{[^{}]*\}', '', text)

# Read the cleaned output
with open('/data/users/brandon/ob1-projects/data_processing/cleaned_output.txt', 'r', encoding='utf-8') as file:
    content = file.read()

# Remove braced text
cleaned_text = remove_braced_text(content)

# Overwrite the cleaned_output.txt file with the final cleaned text
with open('/data/users/brandon/ob1-projects/data_processing/cleaned_output.txt', 'w', encoding='utf-8') as file:
    file.write(cleaned_text)

print("Brace removal complete. Output saved to 'final_cleaned_output.txt'")