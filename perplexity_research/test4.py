import csv
import json
import getpass
import time
from perplexity import Perplexity

def read_companies_from_csv(filename):
    companies = []
    with open(filename, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if "company_name" in row and row["company_name"].strip():
                companies.append(row["company_name"].strip())
    return companies

def collect_raw_search_data(client, company):
    """
    Perform multiple dedicated search API calls to collect raw data about the company.
    No additional processing or parsing done, raw data returned as JSON string.
    """
    print(f"Collecting raw search data for: {company}")
    
    search_queries = [
        f"{company} company headquarters address contact information",
        f"{company} CEO founder executive leadership team",
        f"{company} official website email phone contact",
        f"{company} LinkedIn company profile social media",
        f"{company} employee size staff count company information",
        f"{company} founder CEO executive contact details email phone"
    ]
    
    all_search_results = []
    for i, query in enumerate(search_queries, 1):
        try:
            print(f"Search query {i}: {query[:60]}...")
            search_response = client.search.create(
                query=query,
                max_results=8,
                max_tokens_per_page=1200
            )
            if hasattr(search_response, "results"):
                for result in search_response.results:
                    all_search_results.append({
                        "title": getattr(result, "title", ""),
                        "url": getattr(result, "url", ""),
                        "snippet": getattr(result, "snippet", ""),
                        "content": getattr(result, "content", ""),  # may be empty or partial text
                        "source_query": query
                    })
            time.sleep(0.3)  # to prevent hitting rate limits
        except Exception as e:
            print(f"Search failed for query '{query}': {e}")
            continue
    
    # Return raw collected data as JSON string
    return json.dumps(all_search_results, indent=2)

def main():
    input_file = "inputtest.csv"
    companies = read_companies_from_csv(input_file)
    
    if not companies:
        print("No companies found in the input file.")
        return
    
    # Process only the first company for research purposes
    company = companies[0]
    
    api_key = getpass.getpass("Enter your Perplexity API key: ")
    client = Perplexity(api_key=api_key)
    
    raw_data_json = collect_raw_search_data(client, company)
    
    # Save raw data to JSON file for inspection
    output_filename = f"{company}_raw_search_data.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        f.write(raw_data_json)
    
    print(f"\nRaw search data saved to {output_filename}")
    
if __name__ == "__main__":
    main()
