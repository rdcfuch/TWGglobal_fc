import pandas as pd
import re
from collections import defaultdict

input_file = 'combined_cmbs_rag.txt'
output_file = 'cmbs_rag_output.xlsx'

data = defaultdict(lambda: {'cusips': set(), 'lines': [], 'deal_ids': set()})

pattern = re.compile(r"the deal (\d+) has cusip:([^,]+), the name of this security is ([^,]+),(.+)", re.IGNORECASE)

with open(input_file, 'r', encoding='utf-8') as f:
    for line in f:
        match = pattern.search(line)
        if match:
            deal_id = match.group(1).strip()
            cusip = match.group(2).strip()
            sec_name = match.group(3).strip()
            rest = match.group(4).strip()
            data[sec_name]['cusips'].add(cusip)
            data[sec_name]['deal_ids'].add(deal_id)
            data[sec_name]['lines'].append(rest)

rows = []
for sec_name, info in data.items():
    cusip_list = ', '.join(sorted(info['cusips']))
    deal_id_list = ', '.join(sorted(info['deal_ids']))
    all_info = '\n'.join(info['lines'])
    rag_text = f"Deal ID: {deal_id_list}\nSecurity: {sec_name}\nCUSIPs: {cusip_list}\n{all_info}"
    rows.append({'rag_text': rag_text})

pd.DataFrame(rows).to_excel(output_file, index=False)
print(f"Output written to {output_file}")