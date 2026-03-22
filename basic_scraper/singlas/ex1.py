import pandas as pd
import requests
import getpass
import time
import re
import json

# Output columns including up to two personal injury reference links and two person LinkedIn URLs
output_columns = [
    "Firm Name", "Website", "Address", "City", "State",
    "Personal Injury Reference Link 1", "Personal Injury Reference Link 2",
    "Company LinkedIn Profile", "Person LinkedIn URL 1", "Person LinkedIn URL 2"
]

def google_custom_search(api_key, cx, query, num=10):
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": api_key,
        "cx": cx,
        "q": query,
        "num": num
    }
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Google Search API error: {e}")
        return None

def parse_with_perplexity(api_key, content):
    url = "https://api.perplexity.ai/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    prompt = (
        "Extract the following details exactly in JSON format:\n"
        "{\n"
        "  'firm_name': string,\n"
        "  'address': string,\n"
        "  'city': string,\n"
        "  'state': string,\n"
        "  'linkedin': string (URL),\n"
        "  'personal_injury_references': list of strings (URLs, max 2)\n"
        "}\n\n"
        "Input Text:\n"
        + content
    )

    payload = {
        "model": "sonar-pro",
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 800,
        "temperature": 0
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        message_content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        match = re.search(r'\{.*\}', message_content, re.DOTALL)
        json_text = match.group(0) if match else message_content
        return json.loads(json_text)
    except requests.exceptions.RequestException as e:
        print(f"Perplexity API error: {e}")
        return None
    except json.JSONDecodeError:
        print("Failed to decode JSON from Perplexity response.")
        return None

def combine_search_snippets(items):
    combined = ""
    for item in items:
        combined += item.get("title", "") + "\n"
        combined += item.get("snippet", "") + "\n"
        combined += item.get("link", "") + "\n"
    return combined

def extract_linkedin_urls_from_search(items):
    # Extract linkedin URLs from search items' links and snippets
    linkedin_urls = []
    for item in items:
        link = item.get("link", "")
        snippet = item.get("snippet", "")
        # Check if linkedin.com is in link or snippet
        if "linkedin.com/in/" in link:
            linkedin_urls.append(link)
        elif "linkedin.com/in/" in snippet:
            # Sometimes snippet contains URL
            urls_in_snippet = re.findall(r'https?://[^\s]+', snippet)
            for url in urls_in_snippet:
                if "linkedin.com/in/" in url:
                    linkedin_urls.append(url)
    return linkedin_urls

def main():
    input_file = "input_file.csv"
    df = pd.read_csv(input_file)

    print("Enter Google Custom Search API key:")
    google_api_key = getpass.getpass()
    print("Enter Google Custom Search Engine ID (cx):")
    google_cx = input().strip()
    print("Enter Perplexity API key:")
    perplexity_api_key = getpass.getpass()

    results = []

    for idx, row in df.iterrows():
        website = row["Website"]
        print(f"\nProcessing: {website}")

        search_res = google_custom_search(google_api_key, google_cx, website)
        if not search_res or "items" not in search_res:
            print(f"No search results for {website}")
            result = {col: "" for col in output_columns}
            result["Website"] = website
            results.append(result)
            continue

        combined_text = combine_search_snippets(search_res["items"])
        parsed_data = parse_with_perplexity(perplexity_api_key, combined_text)

        if not parsed_data:
            print(f"Failed to parse data for {website}, saving blank data.")
            parsed_data = {
                "firm_name": "",
                "address": "",
                "city": "",
                "state": "",
                "linkedin": "",
                "personal_injury_references": []
            }

        # Extract personal injury links, max 2
        pir_links = parsed_data.get("personal_injury_references", [])[:2]
        pir_link1 = pir_links[0] if len(pir_links) > 0 else ""
        pir_link2 = pir_links[1] if len(pir_links) > 1 else ""

        # Determine company linkedin
        company_linkedin = parsed_data.get("linkedin", "").strip()

        person_linkedin_urls = []
        if not company_linkedin:
            # Search Google for individual LinkedIn profiles related to company + attorneys/lawyers
            query = f"{parsed_data.get('firm_name', website)} attorney OR lawyer site:linkedin.com/in"
            person_search_res = google_custom_search(google_api_key, google_cx, query, num=10)
            if person_search_res and "items" in person_search_res:
                urls = extract_linkedin_urls_from_search(person_search_res["items"])
                # Limit to max 2
                person_linkedin_urls = urls[:2]

        # Fill output row
        row_result = {
            "Firm Name": parsed_data.get("firm_name", ""),
            "Website": website,
            "Address": parsed_data.get("address", ""),
            "City": parsed_data.get("city", ""),
            "State": parsed_data.get("state", ""),
            "Personal Injury Reference Link 1": pir_link1,
            "Personal Injury Reference Link 2": pir_link2,
            "Company LinkedIn Profile": company_linkedin,
            "Person LinkedIn URL 1": person_linkedin_urls[0] if len(person_linkedin_urls) > 0 else "",
            "Person LinkedIn URL 2": person_linkedin_urls[1] if len(person_linkedin_urls) > 1 else ""
        }

        results.append(row_result)

        time.sleep(2)  # To respect API rate limits

    output_df = pd.DataFrame(results, columns=output_columns)
    output_df.to_csv("newfile.csv", index=False)
    print("\nData collection complete, results saved to newfile.csv")

if __name__ == "__main__":
    main()
