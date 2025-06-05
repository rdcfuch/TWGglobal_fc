import os

def clean_file(filepath):
    """Cleans up a file by removing empty lines and trimming whitespace."""
    with open(filepath, 'r') as file:
        lines = file.readlines()
    
    cleaned_lines = [line.strip() for line in lines if line.strip()]
    
    with open(filepath, 'w') as file:
        file.write('\n'.join(cleaned_lines))

def clean_directory(directory):
    """Cleans up all files in the specified directory and deletes specific 'cmbs_' files."""
    for root, _, files in os.walk(directory):
        for file in files:
            if file.startswith("cmbs_") and file.endswith(('.txt','.csv', '.jsonld', '.cypher')):
                filepath = os.path.join(root, file)
                print(f"Deleting file: {filepath}")
                os.remove(filepath)

if __name__ == "__main__":
    directory = "./"
    clean_directory(directory)
