import re
from collections import defaultdict
import pandas as pd

input_file = "combined_cmbs_rag.txt"
output_file = "combined_by_cusip.xlsx"

cusip_pattern = re.compile(r"cusip:([A-Z0-9]+)")

cusip_lines = defaultdict(list)

with open(input_file, "r", encoding="utf-8") as fin:
    for line in fin:
        match = cusip_pattern.search(line)
        if match:
            cusip = match.group(1)
            cusip_lines[cusip].append(line.strip())

rows = []
for cusip, lines in cusip_lines.items():
    combined = " ||| ".join(lines)
    rows.append({"rag_text": combined})

df = pd.DataFrame(rows)
df.to_excel(output_file, index=False)