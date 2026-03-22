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

# ---- ALLOWED EMPLOYEE SIZE RANGES ----
ALLOWED_EMPLOYEE_RANGES = [
    "1-10", "11-50", "51-200", "201-500", "500+", "1000+", "1001-5000", "5001-10,000", "10000+"
]

def clean_employee_size(raw_size):
    s = raw_size.replace(" ", "").replace(",", "").lower()
    for rng in ALLOWED_EMPLOYEE_RANGES:
        if rng.replace(",", "").lower() == s:
            return rng
    # "1000+" etc.
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

def postprocess_and_clean_csv(input_path, output_path):
    with open(input_path, newline='', encoding='utf-8') as infile, \
         open(output_path, mode='w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            row['Employee Size'] = clean_employee_size(row.get('Employee Size', ""))
            row['Linkedin (company)'] = clean_linkedin_url(row.get('Linkedin (company)', ""))
            row['Linkedin (decision maker)'] = clean_linkedin_url(row.get('Linkedin (decision maker)', ""))
            writer.writerow(row)
    print(f"✅ Cleaned and validated results written to {output_path}")

class FixedOptimizedResearcher:
    def __init__(self, api_key: str):
        try:
            self.client = Perplexity(api_key=api_key)
            logger.info("Perplexity client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Perplexity client: {e}")
            raise
        self.total_search_calls = 0
        self.total_chat_calls = 0
        self.successful_companies = 0
        self.failed_companies = 0
        self.start_time = datetime.now()
        self.last_request_time = 0
        self.min_request_interval = 1.0

    def _rate_limit(self):
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()

    def read_companies_from_csv(self, filename: str) -> List[str]:
        companies = []
        try:
            logger.info(f"Reading companies from {filename}")
            with open(filename, newline='', encoding='utf-8') as csvfile:
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

    def _perform_comprehensive_search(self, company: str) -> List[Dict[str, Any]]:
        logger.info(f"Starting comprehensive search for {company}")
        search_queries = [
            f'"{company}" company headquarters address contact information',
            f'"{company}" CEO founder executive leadership team',
            f'"{company}" official website domain email phone number',
            f'"{company}" LinkedIn company profile social media',
            f'"{company}" employee count staff size revenue funding',
            f'"{company}" management team directors executives contact'
        ]
        all_search_results = []
        for i, query in enumerate(search_queries, 1):
            try:
                self._rate_limit()
                search_response = self.client.search.create(
                    query=query,
                    max_results=10,
                    max_tokens_per_page=1000
                )
                self.total_search_calls += 1
                if hasattr(search_response, 'results') and search_response.results:
                    for result in search_response.results:
                        all_search_results.append({
                            "query_context": query,
                            "title": getattr(result, 'title', ''),
                            "url": getattr(result, 'url', ''),
                            "snippet": getattr(result, 'snippet', ''),
                            "content": getattr(result, 'content', ''),
                            "published_date": getattr(result, 'published_date', ''),
                            "relevance_score": getattr(result, 'score', 0)
                        })
                else:
                    logger.warning(f"Search {i} returned no results for {company}")
            except Exception as e:
                logger.warning(f"Search {i} failed for {company}: {repr(e)}")
                continue
        logger.info(f"Completed {len(all_search_results)} results for {company}")
        return all_search_results

    def _process_search_results(self, company: str, search_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        logger.info(f"Processing search results for {company}")
        limited_results = search_results[:10]
        prompt=(
            f"Extract only these fields as strict JSON with allowed employee size (choose only from {ALLOWED_EMPLOYEE_RANGES}, nothing else): "
            f"Company Name, Website (Email Domain), Employee Size, Address (Company), Country, Email (Company), Phone Number (Company), "
            f"Linkedin (company), First Name, Last Name, Title, Email (decision maker), Linkedin (decision maker), Phone Number (decision maker). "
            "Only include Linkedin fields if the link is a real profile URL. If a field can't be verified, leave it blank. "
            f"Data to analyze: {json.dumps(limited_results)}"
        )
        try:
            self._rate_limit()
            completion = self.client.chat.completions.create(
                messages=[
                    {"role": "system", "content": "Business data cleaning bot. Only respond in strict JSON without extra text."},
                    {"role": "user", "content": prompt}
                ],
                model="sonar-pro",
                disable_search=True,
                temperature=0.1,
                max_tokens=900,
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
            response = completion
            if isinstance(completion, tuple):
                response = completion[0]
            if hasattr(response, 'choices'):
                content = response.choices[0].message.content
                return json.loads(content)
            logger.error(f"Unusual return type from chat completions for {company}: {repr(completion)}")
            return self._create_empty_response(company)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for {company}: {e}")
            return self._create_empty_response(company)
        except Exception as e:
            logger.error(f"Processing failed for {company}: {e}")
            return self._create_empty_response(company)

    def get_company_info_optimized(self, company: str) -> Dict[str, Any]:
        logger.info(f"Starting optimized research for {company}")
        try:
            search_results = self._perform_comprehensive_search(company)
            if not search_results:
                logger.warning(f"No search results for {company}, using fallback")
                return self._fallback_direct_search(company)
            processed_data = self._process_search_results(company, search_results)
            self.successful_companies += 1
            return processed_data
        except Exception as e:
            logger.error(f"Failure processing {company}: {e}")
            self.failed_companies += 1
            return self._create_empty_response(company)

    def _fallback_direct_search(self, company: str) -> Dict[str, Any]:
        logger.info(f"Using fallback method for {company}")
        prompt = (
            f"Strictly return only these fields as JSON (don't estimate values): "
            f"Company Name, Website (Email Domain), Employee Size (must be one of: {ALLOWED_EMPLOYEE_RANGES}), Address (Company), Country, Email (Company), Phone Number (Company), Linkedin (company), First Name, Last Name, Title, Email (decision maker), Linkedin (decision maker), Phone Number (decision maker). No approximations or assumptions. Only real LinkedIn profiles. Leave as blank if not fully verifiable. For: {company}"
        )
        try:
            self._rate_limit()
            completion = self.client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="sonar-pro",
                temperature=0.1,
                enable_search_classifier=True,
                max_tokens=900,
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
            response = completion
            if isinstance(completion, tuple):
                response = completion[0]
            if hasattr(response, 'choices'):
                content = response.choices[0].message.content
                return json.loads(content)
            logger.error(f"Unusual return type from chat completions for {company}: {repr(completion)}")
            return self._create_empty_response(company)
        except Exception as e:
            logger.error(f"Fallback failed for {company}: {e}")
            return self._create_empty_response(company)

    def _create_empty_response(self, company: str) -> Dict[str, str]:
        return {
            "Company Name": company,
            "Website (Email Domain)": "",
            "Employee Size": "",
            "Address (Company)": "",
            "Country": "",
            "Email (Company)": "",
            "Phone Number (Company)": "",
            "Linkedin (company)": "",
            "First Name": "",
            "Last Name": "",
            "Title": "",
            "Email (decision maker)": "",
            "Linkedin (decision maker)": "",
            "Phone Number (decision maker)": ""
        }

    def print_final_statistics(self):
        total = self.successful_companies + self.failed_companies
        success_rate = (self.successful_companies / total * 100) if total > 0 else 0.0
        print("\n" + "="*70)
        print("OPTIMIZED PERPLEXITY API RESEARCH - FINAL STATISTICS")
        print("="*70)
        print(f"Total Processing Time: {datetime.now() - self.start_time}")
        print(f"Companies Processed Successfully: {self.successful_companies}")
        print(f"Companies Failed: {self.failed_companies}")
        print(f"Success Rate: {success_rate:.1f}%")
        print(f"Total Search API Calls: {self.total_search_calls}")
        print(f"Total Chat Completion Calls: {self.total_chat_calls}")
        print(f"Total API Calls: {self.total_search_calls + self.total_chat_calls}")
        print(f"Average API Calls per Company: {(self.total_search_calls + self.total_chat_calls) / total:.1f}" if total > 0 else "N/A")
        print("="*70)

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
    print("🚀 OPTIMIZED PERPLEXITY AI COMPANY RESEARCH TOOL")
    print("=" * 60)
    print("✨ Search API + Chat, automated and error-handled")
    print("🔄 Post-processes for employee size and LinkedIn profile validity")
    print("=" * 60)

    input_file = "inputtest.csv"
    precleaned_output = "company_details_optimized_output.csv"
    final_output = "company_details_verified.csv"

    try:
        api_key = getpass.getpass("Enter your Perplexity API key: ")
        researcher = FixedOptimizedResearcher(api_key)
        companies = researcher.read_companies_from_csv(input_file)import csv
import json
import getpass
import time
import logging
import re
from typing import Dict, List, Any, Optional
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('perplexity_research.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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

class LiteResearcher:
    def __init__(self, api_key: str):
        self.client = Perplexity(api_key=api_key)
        self.start_time = datetime.now()
        self.total_search_calls = 0
        self.total_chat_calls = 0
        self.successful_companies = 0
        self.failed_companies = 0
        self.last_request_time = 0
        self.min_request_interval = 1.0

    def _rate_limit(self):
        now = time.time()
        delta = now - self.last_request_time
        if delta < self.min_request_interval:
            time.sleep(self.min_request_interval - delta)
        self.last_request_time = time.time()

    def get_company_info(self, company: str) -> Dict[str, str]:
        logger.info(f"Processing {company}")
        self._rate_limit()
        # 1. Single targeted search query (broad)
        try:
            search_resp = self.client.search.create(
                query=f"{company} company overview, founders, size, contact, linkedin, decision makers",
                max_results=7,
                max_tokens_per_page=600  # Only snippet and short context
            )
            self.total_search_calls += 1
            results = getattr(search_resp, "results", [])[:7]
            # Prepare a concise context for the chat model
            top_snips = []
            for r in results:
                top_snips.append({
                    "title": getattr(r, "title", ""),
                    "url": getattr(r, "url", ""),
                    "snippet": getattr(r, "snippet", "")
                })
            self._rate_limit()
            # 2. Chat completion on this context only
            prompt = (
                f"You are an expert extracting B2B data. ONLY use these fields: "
                f"Company Name, Website (Email Domain), Employee Size (one of {ALLOWED_EMPLOYEE_RANGES}), "
                f"Address (Company), Country, Email (Company), Phone Number (Company), Linkedin (company), "
                f"First Name, Last Name, Title, Email (decision maker), Linkedin (decision maker), Phone Number (decision maker). "
                f"Only fill a field if strictly found in the provided search result snippets, "
                f"otherwise leave blank. "
                f"For Employee Size, use only allowed range. "
                f"Only include LinkedIn URLs if in proper format and not a generic page. "
                "Data follows in JSON:\n"
                f"{json.dumps(top_snips)}"
            )
            completion = self.client.chat.completions.create(
                messages=[{"role":"system","content":"Strict JSON only, no narration."},{"role":"user","content":prompt}],
                model="sonar-pro", disable_search=True, temperature=0.1, max_tokens=700,
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
            # Clean employee size and LinkedIn before return
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
        return {
            "Company Name": company,
            "Website (Email Domain)": "",
            "Employee Size": "",
            "Address (Company)": "",
            "Country": "",
            "Email (Company)": "",
            "Phone Number (Company)": "",
            "Linkedin (company)": "",
            "First Name": "",
            "Last Name": "",
            "Title": "",
            "Email (decision maker)": "",
            "Linkedin (decision maker)": "",
            "Phone Number (decision maker)": ""
        }

    def print_final_statistics(self):
        total = self.successful_companies + self.failed_companies
        print("\n" + "="*60)
        print("PERPLEXITY LITE RESEARCH SUMMARY")
        print("="*60)
        print(f"Companies Processed Successfully: {self.successful_companies}")
        print(f"Companies Failed: {self.failed_companies}")
        print(f"Total Search API Calls: {self.total_search_calls}")
        print(f"Total Chat Completion Calls: {self.total_chat_calls}")
        print(f"Average API Calls per Company: {(self.total_search_calls+self.total_chat_calls)/total if total>0 else 0:.1f}")
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
        researcher = LiteResearcher(api_key)
        companies = researcher.read_companies_from_csv(input_file) if hasattr(researcher, "read_companies_from_csv") else []
        if not companies:
            # fallback for this "lite" class
            with open(input_file, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                companies = [row.get("company_name","").strip() for row in reader if row.get("company_name","").strip()]
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
                print(f"\n📋 Processing {i + 1}/{end}: {company}")
                info = researcher.get_company_info(company)
                writer.writerow(info)
                print(f"  ✅ Processed: {company}")
        print(f"\n✅ DONE! Results written to {output_file}")
        researcher.print_final_statistics()
        print(f"📝 Log saved to: perplexity_research.log")
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

        print(f"\n🔄 Processing companies {start} to {end} with optimized hybrid approach...")
        print("-" * 60)

        with open(precleaned_output, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()

            for i in range(start - 1, end):
                company = companies[i]
                print(f"\n📋 Processing {i + 1}/{end}: {company}")
                try:
                    info = researcher.get_company_info_optimized(company)
                    required_fields = ["Company Name", "Website (Email Domain)", "Country"]
                    missing_critical = [field for field in required_fields if not info.get(field)]
                    if missing_critical:
                        logger.warning(f"Missing critical fields for {company}: {missing_critical}")
                    writer.writerow(info)
                    print(f"  ✅ Successfully processed: {company}")
                except Exception as e:
                    logger.error(f"Critical error processing {company}: {e}")
                    empty_info = researcher._create_empty_response(company)
                    writer.writerow(empty_info)
                    print(f"  ❌ Failed to process: {company}")

        print(f"\n✅ Processing complete! Raw data written to {precleaned_output}")
        researcher.print_final_statistics()

        print(f"\n🔍 Running post-processing for valid employee size and LinkedIn profiles ...")
        postprocess_and_clean_csv(precleaned_output, final_output)
        print(f"\n📁 Final verified results saved to: {final_output}")
        print(f"📝 Log saved to: perplexity_research.log")

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
