import csv
import json
import getpass
from perplexity import Perplexity


def read_companies_from_csv(filename):
    companies = []
    with open(filename, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if "company_name" in row and row["company_name"].strip():
                companies.append(row["company_name"].strip())
    return companies


def get_company_info(client, company):
    prompt = f"""
    Provide detailed company info in JSON format with these fields:
    1. Company Name
    2. Website (Email Domain)
    3. Employee Size
    4. Address (Company)
    5. Country
    6. Email (Company)
    7. Phone Number (Company)
    8. Linkedin (company)
    9. First Name (decision makers like owner, founder, CEO, MD, directors etc)
    10. Last Name (decision makers like owner, founder, CEO, MD, directors etc)
    11. Title (decision makers like owner, founder, CEO, MD, directors etc)
    12. Email (decision makers like owner, founder, CEO, MD, directors etc)
    13. Linkedin (decision makers like owner, founder, CEO, MD, directors etc)
    14. Phone Number (decision makers like owner, founder, CEO, MD, directors etc)
    for the company: {company}
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
                        "Phone Number (Company)": {"type": "string"},
                        "Linkedin (company)": {"type": "string"},
                        "First Name": {"type": "string"},
                        "Last Name": {"type": "string"},
                        "Title": {"type": "string"},
                        "Email (decision maker)": {"type": "string"},
                        "Linkedin (decision maker)": {"type": "string"},
                        "Phone Number (decision maker)": {"type": "string"},
                    },
                    "required": [
                        "Company Name", "Website (Email Domain)", "Employee Size",
                        "Address (Company)", "Country", "Email (Company)", "Phone Number (Company)",
                        "Linkedin (company)", "First Name", "Last Name", "Title",
                        "Email (decision maker)", "Linkedin (decision maker)", "Phone Number (decision maker)"
                    ]
                }
            }
        }
    )
    return completion.choices[0].message.content


def parse_range_input(users_input, max_companies):
    # Accept input like '10' or '11-25' or '50-100'
    try:
        if '-' in users_input:
            start, end = users_input.split('-')
            start, end = int(start), int(end)
            if start < 1 or end > max_companies or start > end:
                raise ValueError
            return start, end
        else:
            val = int(users_input)
            if val < 1 or val > max_companies:
                raise ValueError
            return 1, val
    except Exception:
        return None


def main():
    input_file = "inputtest.csv"
    output_file = "company_details_output.csv"

    companies = read_companies_from_csv(input_file)
    print(f"Read {len(companies)} companies from {input_file}")

    api_key = getpass.getpass("Enter your Perplexity API key: ")

    client = Perplexity(api_key=api_key)

    while True:
        user_input = input(
            f"Enter number or range of companies to process (e.g., 10 or 11-25) out of {len(companies)}: ")
        rng = parse_range_input(user_input.strip(), len(companies))
        if rng:
            start, end = rng
            break
        else:
            print("Invalid input. Please input a valid number or range within company count.")

    headers = [
        "Company Name", "Website (Email Domain)", "Employee Size",
        "Address (Company)", "Country", "Email (Company)", "Phone Number (Company)",
        "Linkedin (company)", "First Name", "Last Name", "Title",
        "Email (decision maker)", "Linkedin (decision maker)", "Phone Number (decision maker)"
    ]

    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()

        for i in range(start - 1, end):
            company = companies[i]
            print(f"Processing {i + 1}/{end}: {company}")
            try:
                info_str = get_company_info(client, company)
                info = json.loads(info_str)
            except Exception as e:
                print(f"Failed to fetch data for {company}: {e}")
                info = {key: "" for key in headers}
                info["Company Name"] = company

            writer.writerow(info)

    print(f"Data for companies from {start} to {end} written to {output_file}")


if __name__ == "__main__":
    main()
