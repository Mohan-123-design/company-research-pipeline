import os
import csv
import json
from perplexity import Perplexity
import re

# User inputs API key manually here or sets it as an environment variable (recommended)
api_key = os.getenv("PERPLEXITY_API_KEY")
if not api_key:
    api_key = input("Enter your Perplexity API key (it will be kept hidden): ")

# Initialize Perplexity client with the API key securely
client = Perplexity(api_key=api_key)

# CSV headers required for output
headers = [
    "Company Name",
    "Website (Email Domain)",
    "Employee Size",
    "Address (Company)",
    "Country",
    "Email (Company)",
    "Phone Number (Company)"
]

def get_company_info(company):
    prompt = f"""
    Provide detailed company info for {company} with following fields:
    - Company Name
    - Website (Email Domain)
    - Employee Size
    - Address (Company)
    - Country
    - Email (Company)
    - Phone Number (Company)
    Return the answer as JSON.
    """

    completion = client.chat.completions.create(
        messages=[{"role": "user", "content": prompt}],
        model="sonar-pro",
        response_format={
            "type": "json_schema",
            "json_schema": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "Company Name": {"type": "string"},
                        "Website (Email Domain)": {"type": "string"},
                        "Employee Size": {"type": "string"},
                        "Address (Company)": {"type": "string"},
                        "Country": {"type": "string"},
                        "Email (Company)": {"type": "string"},
                        "Phone Number (Company)": {"type": "string"}
                    },
                    "required": [
                        "Company Name",
                        "Website (Email Domain)",
                        "Employee Size",
                        "Address (Company)",
                        "Country",
                        "Email (Company)",
                        "Phone Number (Company)"
                    ]
                }
            }
        }
    )
    return completion.choices[0].message.content

def parse_selection(selection, max_line):
    """
    Parse user input like "10", "11-39", "50-100" or combined like "10, 20-25"
    Returns a set of line indices (0-based).
    """
    indices = set()
    parts = selection.split(",")
    for part in parts:
        part = part.strip()
        if '-' in part:
            start, end = part.split('-')
            try:
                start_i = int(start)
                end_i = int(end)
                # Convert to zero-based index
                indices.update(range(max(start_i, 1)-1, min(end_i, max_line)))
            except ValueError:
                continue
        else:
            try:
                idx = int(part)
                if 1 <= idx <= max_line:
                    indices.add(idx-1)
            except ValueError:
                continue
    return indices

def main():
    # Read companies from inputtest.csv - assuming company names are in first column of CSV
    companies = []
    with open("inputtest.csv", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if row:  # avoid empty lines
                companies.append(row[0].strip())

    # Get user input to select which companies to run (line numbers)
    print(f"Total companies found in inputtest.csv: {len(companies)}")
    selection = input("Enter company line numbers or ranges to process (e.g., '10', '11-39', '50-100', '1,5,20-25'): ")
    selected_indices = parse_selection(selection, len(companies))

    if not selected_indices:
        print("No valid selection made. Exiting.")
        return

    rows = []
    for i in sorted(selected_indices):
        company = companies[i]
        print(f"Fetching data for line {i+1}: {company}")
        try:
            info_json = get_company_info(company)
            info = json.loads(info_json)  # parse JSON string to dict
            rows.append(info)
        except Exception as e:
            print(f"Error fetching data for {company}: {e}")
            # Append blank row with company name for error cases
            blank = {header: "" for header in headers}
            blank["Company Name"] = company
            rows.append(blank)

    # Write to refreshdata.csv
    with open("refreshdata.csv", mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    print("Data fetching completed. Output saved to refreshdata.csv")

if __name__ == "__main__":
    main()
