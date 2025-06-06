import glob

# Pattern to match all target files
input_files = sorted(glob.glob("cmbs_pltr_nodes*.csv"))

# Output file
output_file = "combined_cmbs_pltr_nodes.csv"

with open(output_file, "w", encoding="utf-8") as outfile:
    first_file = True
    for file in input_files:
        with open(file, "r", encoding="utf-8") as infile:
            lines = infile.readlines()
            if first_file:
                outfile.writelines(lines)
                first_file = False
            else:
                outfile.writelines(lines[1:])  # Skip header
            outfile.write("\n")  # Optional: Add newline between files

print(f"Combined {len(input_files)} files into {output_file}")