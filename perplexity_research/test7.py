import csv
import json
import getpass
import time
import logging
import re
from typing import Dict, List, Any, Optional
from datetime import datetime

# ---- LOGGING SETUP ----
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('perplexity_research.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---- PERPLEXITY IMPORT ----
try:
    from perplexity import Perplexity
except ImportError:
    logger.error("Perplexity library not found. Install with: pip install perplexity")
    exit(1)

ALLOWED_EMPLOYEE_RANGES = [
    "1-10", "11-50", "51-200", "201-500", "500+", "1000+", "1001-5000", "5001-10,000", "10000+"
]

def clean_employee_size(raw_size):
    s = raw_size.replace(" ", "").replace(",", "").lower()
    for rng in ALLOWED_EMPLOYEE_RANGES:
        if rng.replace(",", "").lower() == s:
            return rng
    match = re.match(r'^(\d+)\+$', s)
    if match:
        value = int(match.group(1))
        if value >= 10000: return "10000+"
        if value >= 5001: return "5001-10,000"
        if value >= 1001: return "1001-5000"
        if value >= 1000: return "1000+"
        if value >= 500: return "500+"
        if value >= 201: return "201-500"
        if value >= 101: return "101-500"
        if value >= 51: return "51-200"
        if value >= 11: return "11-50"
        return "1-10"
    try:
        value = int(''.join(filter(str.isdigit, s)))
        if value >= 10000:
            return "10000+"
        elif value >= 5001:
            return "5001-10,000"
        elif value >= 1001:
            return "1001-5000"
        elif value >= 1000:
            return "1000+"
        elif value >= 500:
            return "500+"
        elif value >= 201:
            return "201-500"
        elif value >= 101:
            return "101-500"
        elif value >= 51:
            return "51-200"
        elif value >= 11:
            return "11-50"
        else:
            return "1-10"
    except:
        return ""
    return ""

def clean_linkedin_url(url):
    if not url:
        return ""
    url = url.strip()
    m = re.match(r'^https?://(www\.)?linkedin\.com/(company|in)/[\w\-]+/?$', url)
    return url if m else ""

class EfficientResearcher:
    def __init__(self, api_key: str):
        self.client = Perplexity(api_key=api_key)
        self.total_search_calls = 0
        self.total_chat_calls = 0
        self.successful_companies = 0
        self.failed_companies = 0
        self.start_time = datetime.now()
        self.last_request_time = 0
        self.min_request_interval = 1.0

    def _rate_limit(self):
        now = time.time()
        delta = now - self.last_request_time
        if delta < self.min_request_interval:
            time.sleep(self.min_request_interval - delta)
        self.last_request_time = time.time()

    def read_companies_from_csv(self, filename: str) -> List[str]:
        companies = []
        try:
            logger.info(f"Reading companies from {filename}")
            try:
                with open(filename, newline='', encoding='utf-8-sig') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        cname = row.get("company_name", "").strip()
                        if cname:
                            companies.append(cname)
                        else:
                            logger.warning(f"Row with missing or empty company_name: {row}")
            except UnicodeDecodeError:
                logger.info("utf-8-sig decoding failed, retrying with latin1 encoding...")
                with open(filename, newline='', encoding='latin1') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        cname = row.get("company_name", "").strip()
                        if cname:
                            companies.append(cname)
                        else:
                            logger.warning(f"Row with missing or empty company_name: {row}")
            logger.info(f"Loaded {len(companies)} companies from {filename}")
            return companies
        except Exception as e:
            logger.error(f"Error reading CSV file {filename}: {e}")
            raise

    def get_company_info(self, company: str) -> Dict[str, str]:
        logger.info(f"Processing {company}")
        self._rate_limit()
        try:
            search_resp = self.client.search.create(
                query=f"{company} company overview founders size address email phone linkedin CEO MD director",
                max_results=8, max_tokens_per_page=500
            )
            self.total_search_calls += 1
            results = getattr(search_resp, "results", [])[:8]
            top_snips = [{"title": getattr(r,"title",""), "url": getattr(r,"url",""), "snippet": getattr(r,"snippet","")} for r in results]

            self._rate_limit()
            prompt = (
                f"Return only fields: Company Name, Website (Email Domain), Employee Size (choose one from {ALLOWED_EMPLOYEE_RANGES}), "
                f"Address (Company), Country, Email (Company), Phone Number (Company), Linkedin (company), First Name, Last Name, Title, "
                f"Email (decision maker), Linkedin (decision maker), Phone Number (decision maker). "
                "Fill a field only if it is explicitly found in the snippets, otherwise leave it blank. "
                "Employee Size: only use allowed range. LinkedIn fields: only valid profile links. Data follows in JSON:\n"
                f"{json.dumps(top_snips)}"
            )
            completion = self.client.chat.completions.create(
                messages=[{"role":"system","content":"Strict JSON output, no narration."},{"role":"user","content":prompt}],
                model="sonar-pro", disable_search=True, temperature=0.1, max_tokens=600,
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
                                "Phone Number (decision maker)": {"type": "string"}
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
            self.total_chat_calls += 1
            result = completion
            if isinstance(completion, tuple):
                result = completion[0]
            content = result.choices[0].message.content
            j = json.loads(content)
            j['Employee Size'] = clean_employee_size(j.get('Employee Size', ""))
            j['Linkedin (company)'] = clean_linkedin_url(j.get('Linkedin (company)', ""))
            j['Linkedin (decision maker)'] = clean_linkedin_url(j.get('Linkedin (decision maker)', ""))
            self.successful_companies += 1
            return j
        except Exception as e:
            logger.error(f"Failed for {company}: {e}")
            self.failed_companies += 1
            return self._create_empty_response(company)

    def _create_empty_response(self, company: str) -> Dict[str, str]:
        return {k: "" for k in [
            "Company Name", "Website (Email Domain)", "Employee Size",
            "Address (Company)", "Country", "Email (Company)", "Phone Number (Company)",
            "Linkedin (company)", "First Name", "Last Name", "Title",
            "Email (decision maker)", "Linkedin (decision maker)", "Phone Number (decision maker)"
        ]} | {"Company Name": company}

    def print_final_statistics(self):
        total = self.successful_companies + self.failed_companies
        print("\n" + "="*60)
        print("PERPLEXITY LITE RESEARCH SUMMARY")
        print("="*60)
        print(f"Companies Processed Successfully: {self.successful_companies}")
        print(f"Companies Failed: {self.failed_companies}")
        print(f"Total Search API Calls: {self.total_search_calls}")
        print(f"Total Chat Completion Calls: {self.total_chat_calls}")
        print(f"Average API Calls per Company: {(self.total_search_calls+self.total_chat_calls)/(total or 1):.1f}")
        print("="*60)


def parse_range_input(users_input: str, max_companies: int) -> Optional[tuple]:
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
    print("🚀 PERPLEXITY LITE COMPANY RESEARCH")
    print("=" * 50)
    print("Only 2 API calls per company (1 search, 1 chat/extractor)")
    print("=" * 50)

    input_file = "inputtest.csv"
    output_file = "company_details_lite.csv"

    try:
        api_key = getpass.getpass("Enter your Perplexity API key: ")
        researcher = EfficientResearcher(api_key)
        companies = researcher.read_companies_from_csv(input_file)
        print(f"\n📋 Read {len(companies)} companies from {input_file}")

        while True:
            user_input = input(f"\nEnter number or range to process (e.g., 10 or 11-25) out of {len(companies)}: ")
            rng = parse_range_input(user_input.strip(), len(companies))
            if rng:
                start, end = rng
                break
            else:
                print("❌ Invalid input. Please input a valid number or range.")

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
                print(f"\n📋 {i + 1}/{end} Processing: {company}")
                info = researcher.get_company_info(company)
                writer.writerow(info)
                print(f"  ✅ Processed: {company}")

        print(f"\n✅ All done! Results written to {output_file}")
        researcher.print_final_statistics()

    except KeyboardInterrupt:
        print("\n\n⚠️ Process interrupted by user.")
        logger.info("Process interrupted by user")
    except FileNotFoundError as e:
        print(f"\n❌ File error: {e}")
        print("Please ensure inputtest.csv exists")
    except Exception as e:
        logger.error(f"Critical error in main: {e}")
        print(f"\n❌ Critical error: {e}")
        raise


if __name__ == "__main__":
    main()

