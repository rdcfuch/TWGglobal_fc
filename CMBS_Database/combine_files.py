import glob

# Pattern to match all target files
input_files = sorted(glob.glob("cmbs_rag*.txt"))

# Output file
output_file = "combined_cmbs_rag.txt"

with open(output_file, "w", encoding="utf-8") as outfile:
    for file in input_files:
        with open(file, "r", encoding="utf-8") as infile:
            outfile.write(infile.read())
            outfile.write("\n")  # Optional: Add newline between files

print(f"Combined {len(input_files)} files into {output_file}")