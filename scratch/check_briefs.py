import pandas as pd
import glob
df = pd.read_parquet(glob.glob('data/gold/intelligence_briefs/*.parquet')[0])
print(f"Total: {len(df)}")
ghosts = df[df.verdict.str.contains('GHOST PUMP', na=False)]
errors = df[df.verdict.str.contains('Error', na=False)]
real = df[~df.verdict.str.contains('GHOST PUMP|Error', na=False)]
print(f"Real Verdicts: {len(real)}")
print(f"Ghost Pumps: {len(ghosts)}")
print(f"Errors: {len(errors)}")
print("\n=== SAMPLE VERDICTS ===")
print(real[['ticker', 'verdict']].head(3).to_string())
