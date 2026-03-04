import pandas as pd

input_file = "Traffic_Violations 2.csv"
output_file = "Traffic_Sample.csv"
target_count = 2500 # 2500 Citations + 2500 Warnings = 5000 rows

citations = []
warnings = []

print("🚀 Searching for 5,000 balanced rows...")

for chunk in pd.read_csv(input_file, chunksize=50000, low_memory=False):
    # Standardize column names
    chunk.columns = chunk.columns.str.strip()
    
    c_chunk = chunk[chunk['Violation Type'] == 'Citation']
    w_chunk = chunk[chunk['Violation Type'] == 'Warning']
    
    citations.append(c_chunk)
    warnings.append(w_chunk)
    
    c_total = sum(len(x) for x in citations)
    w_total = sum(len(x) for x in warnings)
    
    print(f"Progress: {c_total} Citations, {w_total} Warnings found...", end="\r")
    
    if c_total >= target_count and w_total >= target_count:
        break

# Merge and shuffle
df_c = pd.concat(citations).head(target_count)
df_w = pd.concat(warnings).head(target_count)
df_final = pd.concat([df_c, df_w]).sample(frac=1).reset_index(drop=True)

df_final.to_csv(output_file, index=False)
print(f"\n\n✅ Done! Saved 5,000 balanced rows to {output_file}")