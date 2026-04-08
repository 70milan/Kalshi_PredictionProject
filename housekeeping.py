import os
import requests
import csv

# ─────────────────────────────────────────────
# 1. STRIP EMOJIS FROM KALSHI SCRIPTS
# ─────────────────────────────────────────────
target_dir = r"c:\Data Engineering\codeprep\predection_project\native_ingestion"
files_to_clean = [
    "flat_kalshi_markets_active.py", 
    "flat_kalshi_markets_historical.py", 
    "flat_kalshi_daily_settlement.py"
]

emojis = ['⚠️', '❌', '📋', '📡', '✅', '💾', '🔍', '☕', '⏭️', '📄']

for fname in files_to_clean:
    filepath = os.path.join(target_dir, fname)
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        for e in emojis:
            content = content.replace(e + '  ', '')
            content = content.replace(e + ' ', '')
            content = content.replace(e, '')
            
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Stripped emojis from: {fname}")


# ─────────────────────────────────────────────
# 2. GENERATE EXPANDED REFERENCE CSVs
# ─────────────────────────────────────────────
PROJECT_ROOT = r"c:\Data Engineering\codeprep\predection_project"
REF_DIR = os.path.join(PROJECT_ROOT, "reference")
os.makedirs(REF_DIR, exist_ok=True)

# Extended CAMEO Event Codes (~65 Core Sub-levels)
cameo = [
    ("01", "MAKE PUBLIC STATEMENT"), ("010", "Make statement, not specified below"), 
    ("011", "Decline comment"), ("012", "Make pessimistic comment"),
    ("013", "Make optimistic comment"), ("014", "Consider policy option"),
    ("02", "APPEAL"), ("020", "Appeal, not specified below"), 
    ("021", "Appeal for material cooperation"), ("0211", "Appeal for economic cooperation"),
    ("025", "Appeal to yield"), ("027", "Appeal for policy change"),
    ("03", "EXPRESS INTENT TO COOPERATE"), ("031", "Express intent to engage in material cooperation"),
    ("04", "CONSULT"), ("040", "Consult, not specified below"), ("041", "Discuss by telephone"),
    ("042", "Make a visit"), ("043", "Host a visit"), ("044", "Meet at a 'third' location"),
    ("05", "ENGAGE IN DIPLOMATIC COOPERATION"), ("050", "Engage in diplomatic cooperation"),
    ("06", "ENGAGE IN MATERIAL COOPERATION"), ("060", "Engage in material cooperation"),
    ("07", "PROVIDE AID"), ("070", "Provide aid, not specified below"), 
    ("071", "Provide economic aid"), ("072", "Provide military aid"),
    ("08", "YIELD"), ("080", "Yield, not specified below"), ("081", "Ease administrative sanctions"),
    ("09", "INVESTIGATE"), ("090", "Investigate, not specified below"),
    ("10", "DEMAND"), ("100", "Demand, not specified below"),
    ("11", "DISAPPROVE"), ("110", "Disapprove, not specified below"),
    ("111", "Criticize or denounce"), ("112", "Accuse"),
    ("12", "REJECT"), ("120", "Reject, not specified below"),
    ("13", "THREATEN"), ("130", "Threaten, not specified below"),
    ("131", "Threaten non-force"), ("138", "Threaten with military force"),
    ("14", "PROTEST"), ("140", "Engage in political dissent"),
    ("141", "Demonstrate or rally"), ("145", "Protest violently, riot"),
    ("15", "EXHIBIT FORCE POSTURE"), ("150", "Demonstrate military power"),
    ("16", "REDUCE RELATIONS"), ("160", "Reduce relations"),
    ("17", "COERCE"), ("170", "Coerce, not specified below"),
    ("171", "Seize or damage property"), ("172", "Impose administrative sanctions"),
    ("173", "Arrest, detain, or charge"), ("174", "Expel or deport individuals"),
    ("18", "ASSAULT"), ("180", "Use unconventional violence"),
    ("181", "Abduct, hijack, or take hostage"), ("182", "Physically assault"),
    ("183", "Conduct suicide, car, or other non-military bombing"),
    ("186", "Assassinate"),
    ("19", "FIGHT"), ("190", "Use conventional military force"),
    ("193", "Fight with small arms and light weapons"),
    ("194", "Fight with artillery and tanks"),
    ("20", "USE UNCONVENTIONAL MASS VIOLENCE"), ("200", "Use unconventional mass violence")
]

# Extended FIPS 10-4 Country Codes (~90 Core Countries)
fips = [
    ("US", "United States"), ("UK", "United Kingdom"), ("CH", "China"),
    ("RS", "Russia"), ("IR", "Iran"), ("IS", "Israel"), ("IN", "India"),
    ("PK", "Pakistan"), ("FR", "France"), ("GM", "Germany"), ("JA", "Japan"),
    ("KS", "South Korea"), ("KN", "North Korea"), ("SY", "Syria"),
    ("AF", "Afghanistan"), ("UA", "Ukraine"), ("TW", "Taiwan"), ("IZ", "Iraq"),
    ("MX", "Mexico"), ("CA", "Canada"), ("BR", "Brazil"), ("AR", "Argentina"),
    ("SP", "Spain"), ("IT", "Italy"), ("EG", "Egypt"), ("SA", "Saudi Arabia"),
    ("TU", "Turkey"), ("AU", "Australia"), ("NZ", "New Zealand"),
    ("ZA", "South Africa"), ("NG", "Nigeria"), ("KE", "Kenya"),
    ("CU", "Cuba"), ("VE", "Venezuela"), ("CO", "Colombia"), ("PE", "Peru"),
    ("ID", "Indonesia"), ("MY", "Malaysia"), ("RP", "Philippines"),
    ("TH", "Thailand"), ("VM", "Vietnam"), ("CB", "Cambodia"), ("BM", "Myanmar"),
    ("PL", "Poland"), ("HU", "Hungary"), ("RO", "Romania"), ("BU", "Bulgaria"),
    ("GR", "Greece"), ("SW", "Sweden"), ("NO", "Norway"), ("DA", "Denmark"),
    ("FI", "Finland"), ("EI", "Ireland"), ("SZ", "Switzerland"), ("AU", "Austria"),
    ("BE", "Belgium"), ("NL", "Netherlands"), ("PO", "Portugal"), ("DZ", "Algeria"),
    ("LY", "Libya"), ("MO", "Morocco"), ("SU", "Sudan"), ("ET", "Ethiopia"),
    ("SO", "Somalia"), ("YM", "Yemen"), ("OM", "Oman"), ("AE", "United Arab Emirates"),
    ("QA", "Qatar"), ("KU", "Kuwait"), ("BA", "Bahrain"), ("JO", "Jordan"),
    ("LE", "Lebanon"), ("TS", "Tunisia"), ("ZI", "Zimbabwe"), ("AO", "Angola"),
    ("MZ", "Mozambique"), ("CG", "Congo (DRC)"), ("RW", "Rwanda"), ("UG", "Uganda"),
    ("TZ", "Tanzania"), ("GH", "Ghana"), ("SG", "Senegal"), ("IV", "Cote d'Ivoire")
]

with open(os.path.join(REF_DIR, "cameo_eventcodes.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["CAMEO", "DESCRIPTION"])
    writer.writerows(cameo)
print("Expanded cameo_eventcodes.csv generated.")

with open(os.path.join(REF_DIR, "fips_countries.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["FIPS", "COUNTRY"])
    writer.writerows(fips)
print("Expanded fips_countries.csv generated.")
