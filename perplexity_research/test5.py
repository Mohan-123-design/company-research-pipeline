import csv
import json
import getpass
import time
import logging
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
                row_count = 0
                for row in reader:
                    row_count += 1
                    if "company_name" in row and row["company_name"].strip():
                        companies.append(row["company_name"].strip())
                    else:
                        logger.warning(f"Row {row_count}: Missing or empty company_name")
            logger.info(f"Loaded {len(companies)} companies from {filename}")
            return companies
        except FileNotFoundError:
            logger.error(f"File {filename} not found")
            raise
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
        successful_searches = 0
        for i, query in enumerate(search_queries, 1):
            try:
                self._rate_limit()
                logger.debug(f"Search {i}/{len(search_queries)}: {query[:60]}...")
                search_response = self.client.search.create(
                    query=query,
                    max_results=15,
                    max_tokens_per_page=2000
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
                    successful_searches += 1
                    logger.debug(f"Search {i} successful: {len(search_response.results)} results")
                else:
                    logger.warning(f"Search {i} returned no results")
            except Exception as e:
                logger.warning(f"Search {i} failed for {company}: {e}")
                continue
        logger.info(f"Completed {successful_searches}/{len(search_queries)} searches, collected {len(all_search_results)} results")
        return all_search_results
    
    def _process_search_results(self, company: str, search_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        logger.info(f"Processing search results for {company}")
        limited_results = search_results[:25]
        processing_prompt = f"""
You are an expert business intelligence analyst. Extract and structure company information with maximum accuracy based on this data about {company}:
{json.dumps(limited_results, indent=2)}
Extract ONLY the specified JSON fields, cross-reference for accuracy, use latest sources.
"""
        try:
            self._rate_limit()
            completion = self.client.chat.completions.create(
                messages=[
                    {"role": "system", "content": "Expert business intelligence analyst."},
                    {"role": "user", "content": processing_prompt}
                ],
                model="sonar-pro",
                disable_search=True,
                temperature=0.1,
                max_tokens=4000,
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
            return json.loads(completion.choices[0].message.content)
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
        prompt = f"""
Research and provide detailed company info for {company}.
Use accurate, verified info with these fields only.
"""
        try:
            self._rate_limit()
            completion = self.client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="sonar-pro",
                temperature=0.1,
                enable_search_classifier=True,
                response_format={"type": "json_schema", "json_schema": {"schema": {"type": "object", "properties": {
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
                }, "required": [
                    "Company Name", "Website (Email Domain)", "Employee Size",
                    "Address (Company)", "Country", "Email (Company)", "Phone Number (Company)",
                    "Linkedin (company)", "First Name", "Last Name", "Title",
                    "Email (decision maker)", "Linkedin (decision maker)", "Phone Number (decision maker)"
                ]}}}),
            
            self.total_chat_calls += 1
            return json.loads(completion.choices[0].message.content)
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
        print(f"Average API Calls per Company: {((self.total_search_calls + self.total_chat_calls) / total):.1f}" if total > 0 else "N/A")
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
    print("🚀 OPTIMIZED PERPLEXITY API COMPANY RESEARCH TOOL")
    print("=" * 60)
    print("✨ Enhanced with Search API + Chat Completions for maximum accuracy")
    print("🔄 Automatically uses hybrid approach without configuration needed")
    print("=" * 60)
    
    input_file = "inputtest.csv"
    output_file = "company_details_optimized_output.csv"


    try:
        api_key = getpass.getpass("Enter your Perplexity API key: ")
        researcher = FixedOptimizedResearcher(api_key)
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


        print(f"\n🔄 Processing companies {start} to {end} with optimized hybrid approach...")
        print("-" * 60)


        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
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


        print(f"\n✅ Processing complete! Data written to {output_file}")
        researcher.print_final_statistics()
        print(f"\n📁 Results saved to: {output_file}")
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
