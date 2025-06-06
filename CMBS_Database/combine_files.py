import glob
import pandas as pd


def combine_csv_files(pattern="cmbs_pltr_nodes*.csv", output_file="combined_cmbs_pltr_nodes.csv"):
    """
    Combine multiple CSV files matching a pattern into a single output file.
    
    Args:
        pattern (str): Glob pattern to match input files
        output_file (str): Path to the output combined file
        
    Returns:
        str: Path to the combined output file
    """
    # Get all files matching the pattern
    input_files = sorted(glob.glob(pattern))
    
    if not input_files:
        print(f"No files found matching pattern: {pattern}")
        return None
    
    # Combine files
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
    return output_file


def convert_to_excel(csv_file, excel_file=None, separator='|'):
    """
    Convert a CSV file to Excel format using the specified separator.
    
    Args:
        csv_file (str): Path to the CSV file to convert
        excel_file (str, optional): Path to the output Excel file. If None, uses the CSV filename with .xlsx extension
        separator (str, optional): Delimiter used in the CSV file. Defaults to '|'
    """
    if excel_file is None:
        excel_file = csv_file.replace('.csv', '.xlsx')
    
    # Read the CSV file with the specified separator
    df = pd.read_csv(csv_file, sep=separator)
    
    # Write to Excel file
    df.to_excel(excel_file, index=False)
    
    print(f"Converted {csv_file} to Excel format: {excel_file}")


def main():
    """
    Main function to run the file combination and conversion process.
    """
    # Combine CSV files
    combined_file = combine_csv_files()
    
    # Convert the combined file to Excel format
    if combined_file:
        convert_to_excel(combined_file)


# Execute main function when script is run directly
if __name__ == "__main__":
    main()
