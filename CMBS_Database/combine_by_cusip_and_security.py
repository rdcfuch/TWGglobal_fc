import collections
import pandas as pd
import re

def combine_lines_by_cusip_and_security(input_path, output_excel_path):
    grouped = collections.defaultdict(list)
    cusip_pattern = re.compile(r"cusip:([^,]+)")
    security_pattern = re.compile(r"the name of this security is ([^,]+)")
    with open(input_path, 'r', encoding='utf-8') as infile:
        for line in infile:
            line = line.strip()
            if not line:
                continue
            try:
                cusip_match = cusip_pattern.search(line)
                security_match = security_pattern.search(line)
                if cusip_match and security_match:
                    cusip = cusip_match.group(1).strip()
                    security = security_match.group(1).strip()
                    key = (cusip, security)
                    grouped[key].append(line)
            except Exception as e:
                print(f"Skipping line due to error: {e}\\n{line}")
    # Write to Excel file
    records = []
    for (cusip, security), lines in grouped.items():
        combined = ' || '.join(lines)
        records.append({'rag_text': combined})
    df = pd.DataFrame(records)
    df.to_excel(output_excel_path, index=False)

if __name__ == "__main__":
    input_file = "/Users/jackyfox/PycharmProjects/TWGglobal_fc/CMBS_Database/combined_cmbs_rag.txt"
    output_excel_file = "/Users/jackyfox/PycharmProjects/TWGglobal_fc/CMBS_Database/combined_by_cusip_and_security.xlsx"
    combine_lines_by_cusip_and_security(input_file, output_excel_file)