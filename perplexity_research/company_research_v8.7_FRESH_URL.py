# ============================================================================
# COMPANY RESEARCH AUTOMATION - PERPLEXITY v8.7 - FRESH URL NAVIGATION
# ============================================================================
# UPDATED APPROACH:
# 1. ✓ Fixed: Use FRESH URL navigation for each company (no follow-ups)
# 2. ✓ Better reliability: Each company gets clean slate
# 3. ✓ Simpler process: Navigate → Search → Extract → Repeat
# 4. ✓ No follow-up chain failures: Each search is independent
# 5. ✓ Working flow preserved: Same output, better execution
#
# SETUP INSTRUCTIONS:
# 1. Replace YOUR_THREAD_LINK in SEARCH_URL (around line 50)
# 2. Ensure creden.json and .env are in same folder as this script
# 3. Run: python company_research_v8.7_FRESH_URL.py
#
# ============================================================================

import asyncio
import os
import datetime
import logging
import re
import time
from dotenv import load_dotenv
from getpass import getpass
from playwright.async_api import async_playwright
from google import genai
from google.genai.types import Content, Part
import gspread
from oauth2client.service_account import ServiceAccountCredentials

load_dotenv()

# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

# SEARCH URL - Use basic Perplexity search (fresh navigation each time)
SEARCH_URL = "https://www.perplexity.ai/spaces/company-details-Ygq6oOZCSHyasw6gkdTkrQ"

# ============================================================================
# SYSTEM CONFIGURATION
# ============================================================================

TITLE = "="*80
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"company_research_perplexity_{timestamp}.log"
screenshot_dir = f"perplexity_screenshots_{timestamp}"

if not os.path.exists(screenshot_dir):
    os.makedirs(screenshot_dir)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

SCREEN_WIDTH = 1600
SCREEN_HEIGHT = 1080
PAGE_LOAD_TIMEOUT = 95000
PERPLEXITY_LOAD_TIME = 6

# ============================================================================
# GOOGLE SHEETS SETUP
# ============================================================================

def setup_google_sheets_v87():
    """Setup Google Sheets"""
    try:
        logger.info("Setting up Google Sheets...")
        print("Connecting to Google Sheets...")
        
        keyfile = os.getenv("GOOGLE_SHEETS_KEYFILE", "creden.json")
        spreadsheet_id = os.getenv("SPREADSHEET_ID")
        
        if not keyfile or not spreadsheet_id:
            print("ERROR: Missing credentials")
            logger.error("Missing GOOGLE_SHEETS_KEYFILE or SPREADSHEET_ID")
            return None, None
        
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        
        creds = ServiceAccountCredentials.from_json_keyfile_name(keyfile, scope)
        client = gspread.authorize(creds)
        
        spreadsheet = client.open_by_key(spreadsheet_id)
        
        try:
            sheet = spreadsheet.worksheet("Company Research")
        except:
            sheet = spreadsheet.add_worksheet(title="Company Research", rows=500, cols=20)
        
        headers = [
            "Company Name", "Website", "Address", "City", "State", "Country", 
            "Zipcode", "Email", "Phone Number", "LinkedIn URL", "Industry", 
            "Company Size", "Status", "Date Updated", "Notes"
        ]
        
        first_row = sheet.row_values(1)
        
        if not first_row or first_row[0] != "Company Name":
            for i, h in enumerate(headers, 1):
                sheet.update_cell(1, i, h)
        
        print("✓ OK - Sheet connected")
        return sheet, client
        
    except Exception as e:
        logger.error(f"Setup error: {e}")
        print(f"ERROR: {e}")
        return None, None

# ============================================================================
# GEMINI SETUP
# ============================================================================

def get_gemini_client():
    """Get Gemini client"""
    try:
        gemini_api_key = os.getenv("GEMINI_API_KEY")
        
        if not gemini_api_key:
            print("ERROR: GEMINI_API_KEY not found in .env")
            logger.error("Missing GEMINI_API_KEY")
            return None
        
        return genai.Client(api_key=gemini_api_key)
        
    except Exception as e:
        logger.error(f"Gemini error: {e}")
        print(f"ERROR: Gemini - {e}")
        return None

# ============================================================================
# NOTES GENERATION (v8.7)
# ============================================================================

def generate_notes_v87(results):
    """Generate notes based on missing data"""
    try:
        if not results:
            return "No data extracted"
        
        fields_to_check = {
            "website": "Website",
            "address": "Address",
            "city": "City",
            "state": "State",
            "country": "Country",
            "zipcode": "Zipcode",
            "email": "Email",
            "phone": "Phone",
            "linkedin": "LinkedIn",
            "industry": "Industry",
            "size": "Company Size"
        }
        
        missing_fields = []
        
        for field_key, field_name in fields_to_check.items():
            if field_key in results:
                value = results[field_key]
                
                if value == "NOT FOUND" or value is None or value == "":
                    missing_fields.append(field_name)
        
        if not missing_fields:
            notes = "✓ All fields extracted successfully"
        elif len(missing_fields) <= 3:
            missing_str = " | ".join([f"{field}: NOT FOUND" for field in missing_fields])
            notes = f"Partial: {missing_str}"
        else:
            count = len(missing_fields)
            notes = f"{count} fields missing: {', '.join(missing_fields[:3])}..."
        
        logger.info(f"Generated notes: {notes}")
        return notes
        
    except Exception as e:
        logger.error(f"Notes generation error: {e}")
        return "Error generating notes"

# ============================================================================
# EXTRACTION FUNCTIONS
# ============================================================================

def extract_country_from_text(text: str) -> str:
    """Extract country from address text"""
    try:
        if not text:
            return "NOT FOUND"
        
        text_lower = text.lower()
        countries = {
            "United States": ["united states", "usa", "u.s.", "us", "america"],
            "Canada": ["canada", "cdn"],
            "India": ["india", "indian"],
            "United Kingdom": ["uk", "united kingdom", "england", "scotland"],
            "Australia": ["australia", "aussie"],
            "Germany": ["germany", "deutschland"],
            "France": ["france", "french"],
            "Japan": ["japan", "japanese"],
            "China": ["china", "chinese"],
            "Singapore": ["singapore"],
            "Ireland": ["ireland", "irish"],
            "Netherlands": ["netherlands", "dutch"],
            "Switzerland": ["switzerland", "swiss"],
            "Sweden": ["sweden", "swedish"],
            "Norway": ["norway", "norwegian"],
        }
        
        for country, keywords in countries.items():
            for keyword in keywords:
                if keyword in text_lower:
                    return country
        
        return "NOT FOUND"
        
    except Exception as e:
        logger.error(f"Country extraction error: {e}")
        return "NOT FOUND"

def extract_with_gemini_vision(client, screenshot_path, extraction_type, company_name):
    """Use Gemini Vision to extract from screenshot"""
    try:
        if not os.path.exists(screenshot_path):
            return None
        
        with open(screenshot_path, "rb") as f:
            image_data = f.read()
        
        prompts = {
            "website": f"Extract ONLY the official website URL for {company_name}. Return just the URL starting with https. If not found, return NOT FOUND.",
            "address": f"Extract the COMPLETE headquarters address for {company_name}. Return in format Street, City, State Zipcode",
            "city": f"Extract ONLY the city name. Return just the city name.",
            "state": f"Extract ONLY the state/province name or abbreviation. Return 2-letter code or full name.",
            "country": f"Extract ONLY the country name. Return just the country name.",
            "zipcode": f"Extract ONLY the postal/zip code. Return just the number.",
            "email": f"Extract the MAIN contact email. Return just the email address.",
            "phone": f"Extract the MAIN phone number. Return in format XXX-XXX-XXXX",
            "linkedin": f"Extract the LinkedIn company page URL. Return just the full URL starting with https",
            "industry": f"What industry is {company_name} in? Return just the industry name.",
            "size": f"How many employees does {company_name} have? Return just the number.",
        }
        
        prompt = prompts.get(extraction_type, "Extract the relevant information.")
        
        response = client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents=[
                Content(
                    role="user",
                    parts=[
                        Part(text=prompt),
                        Part.from_bytes(data=image_data, mime_type="image/png")
                    ]
                )
            ]
        )
        
        result = response.text.strip() if response else None
        
        return result
        
    except Exception as e:
        logger.error(f"Vision error: {e}")
        return None

def extract_website_v87(text, screenshot_path, client, company_name):
    """Extract website"""
    try:
        if client and screenshot_path:
            vision_result = extract_with_gemini_vision(client, screenshot_path, "website", company_name)
            if vision_result and vision_result != "NOT FOUND" and "http" in vision_result:
                return vision_result.split()[0]
        
        if not text:
            return "NOT FOUND"
        
        text_clean = " ".join(text.split())
        pattern = r"https?://(?:www\.)?[a-zA-Z0-9\-\.]+\.[a-zA-Z0-9\.\-]+"
        matches = re.findall(pattern, text_clean)
        
        if matches:
            for url in matches:
                if not any(x in url.lower() for x in ["facebook", "twitter", "instagram"]):
                    return url.rstrip()
        
        return "NOT FOUND"
        
    except:
        return "NOT FOUND"

def extract_address_v87(text, screenshot_path, client, company_name):
    """Extract complete address"""
    try:
        result = {
            "address": "NOT FOUND",
            "city": "NOT FOUND",
            "state": "NOT FOUND",
            "country": "NOT FOUND",
            "zipcode": "NOT FOUND"
        }
        
        if client and screenshot_path:
            vision_addr = extract_with_gemini_vision(client, screenshot_path, "address", company_name)
            vision_city = extract_with_gemini_vision(client, screenshot_path, "city", company_name)
            vision_state = extract_with_gemini_vision(client, screenshot_path, "state", company_name)
            vision_country = extract_with_gemini_vision(client, screenshot_path, "country", company_name)
            vision_zip = extract_with_gemini_vision(client, screenshot_path, "zipcode", company_name)
            
            if vision_addr and vision_addr != "NOT FOUND":
                result["address"] = vision_addr[:200]
            if vision_city and vision_city != "NOT FOUND":
                result["city"] = vision_city.strip()
            if vision_state and vision_state != "NOT FOUND":
                result["state"] = vision_state.strip()
            if vision_country and vision_country != "NOT FOUND":
                result["country"] = vision_country.strip()
            if vision_zip and vision_zip != "NOT FOUND":
                result["zipcode"] = vision_zip.strip()
            
            if result["address"] != "NOT FOUND":
                return result
        
        if not text:
            return result
        
        text_clean = " ".join(text.split())
        
        if result["country"] == "NOT FOUND":
            result["country"] = extract_country_from_text(text_clean)
        
        pattern = r"([\d\w\s\.]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd)\.?),?\s+([A-Za-z\s]+),?\s+([A-Za-zA-Z]{2})\s+(\d{5})"
        match = re.search(pattern, text_clean)
        
        if match:
            result["address"] = match.group(1).strip()
            result["city"] = match.group(2).strip()
            result["state"] = match.group(3).strip()
            result["zipcode"] = match.group(4).strip()
        
        return result
        
    except:
        return {
            "address": "NOT FOUND",
            "city": "NOT FOUND",
            "state": "NOT FOUND",
            "country": "NOT FOUND",
            "zipcode": "NOT FOUND"
        }

def extract_phone_v87(text, screenshot_path, client, company_name):
    """Extract phone"""
    try:
        if client and screenshot_path:
            vision_phone = extract_with_gemini_vision(client, screenshot_path, "phone", company_name)
            if vision_phone and vision_phone != "NOT FOUND":
                digits = re.sub(r"\D", "", vision_phone)
                if len(digits) >= 10:
                    return vision_phone
        
        if not text:
            return "NOT FOUND"
        
        text_clean = " ".join(text.split())
        pattern = r"(?:\+1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})"
        match = re.search(pattern, text_clean)
        
        if match:
            return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
        
        return "NOT FOUND"
        
    except:
        return "NOT FOUND"

def extract_email_v87(text, screenshot_path, client, company_name):
    """Extract email"""
    try:
        if client and screenshot_path:
            vision_email = extract_with_gemini_vision(client, screenshot_path, "email", company_name)
            if vision_email and "@" in vision_email and vision_email != "NOT FOUND":
                return vision_email.strip().split()[0]
        
        if not text:
            return "NOT FOUND"
        
        pattern = r"[a-zA-Z0-9.\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
        matches = re.findall(pattern, text)
        
        if matches:
            for email in matches:
                if not any(x in email.lower() for x in ["test", "example"]):
                    return email
        
        return "NOT FOUND"
        
    except:
        return "NOT FOUND"

def extract_linkedin_v87(text, screenshot_path, client, company_name):
    """Extract LinkedIn"""
    try:
        if client and screenshot_path:
            vision_linkedin = extract_with_gemini_vision(client, screenshot_path, "linkedin", company_name)
            if vision_linkedin and "linkedin.com/company" in vision_linkedin:
                return vision_linkedin.split()[0]
        
        if not text:
            return "NOT FOUND"
        
        pattern = r"https?://(?:www\.)?linkedin\.com/company/[a-zA-Z0-9\-]+"
        matches = re.findall(pattern, text, re.IGNORECASE)
        
        if matches:
            return matches[0]
        
        return "NOT FOUND"
        
    except:
        return "NOT FOUND"

def extract_industry_v87(text, screenshot_path, client, company_name):
    """Extract industry"""
    try:
        if client and screenshot_path:
            vision_industry = extract_with_gemini_vision(client, screenshot_path, "industry", company_name)
            if vision_industry and vision_industry != "NOT FOUND" and len(vision_industry) < 50:
                return vision_industry.strip()
        
        if not text:
            return "NOT FOUND"
        
        text_lower = text.lower()
        industries = {
            "Technology": ["technology", "tech", "software", "saas", "cloud", "ai"],
            "Finance": ["financial", "finance", "banking", "fintech"],
            "Healthcare": ["health", "medical", "hospital", "pharma"],
            "Retail": ["retail", "commerce", "shopping"],
            "Manufacturing": ["manufacturing", "factory", "production"],
        }
        
        for industry, keywords in industries.items():
            if any(kw in text_lower for kw in keywords):
                return industry
        
        return "NOT FOUND"
        
    except:
        return "NOT FOUND"

def extract_size_v87(text, screenshot_path, client, company_name):
    """Extract company size"""
    try:
        if client and screenshot_path:
            vision_size = extract_with_gemini_vision(client, screenshot_path, "size", company_name)
            if vision_size and vision_size != "NOT FOUND":
                digits = re.sub(r"\D", "", vision_size)
                if digits and 1 <= int(digits) <= 10000000:
                    return digits
        
        if not text:
            return "NOT FOUND"
        
        text_clean = " ".join(text.split())
        pattern = r"([0-9,]+)\s+(?:employees?|staff|people)"
        match = re.search(pattern, text_clean, re.IGNORECASE)
        
        if match:
            size_str = match.group(1).replace(",", "")
            if 1 <= int(size_str) <= 10000000:
                return size_str
        
        return "NOT FOUND"
        
    except:
        return "NOT FOUND"

# ============================================================================
# FRESH URL NAVIGATION - SIMPLIFIED APPROACH
# ============================================================================

async def navigate_and_search_fresh(page, company_name, search_num, client):
    """Navigate to fresh URL and search for company - SIMPLIFIED"""
    try:
        print(f"[{search_num}/5] Navigating and searching...")
        
        # Navigate to fresh search URL
        logger.info(f"Navigating to: {SEARCH_URL}")
        await page.goto(
            SEARCH_URL,
            wait_until="domcontentloaded",
            timeout=PAGE_LOAD_TIMEOUT
        )
        
        await asyncio.sleep(PERPLEXITY_LOAD_TIME)
        logger.info("✓ Page loaded successfully")
        
        # Find search input
        selectors = [
            'textarea',
            'input[type="text"]',
            'div[role="textbox"]',
        ]
        
        search_input = None
        for selector in selectors:
            try:
                elements = await page.query_selector_all(selector)
                if elements and len(elements) > 0:
                    for elem in elements:
                        try:
                            if await elem.is_visible() and await elem.is_enabled():
                                search_input = elem
                                break
                        except:
                            continue
                if search_input:
                    break
            except:
                continue
        
        if not search_input:
            logger.error(f"Search input not found")
            return None, ""
        
        # Type company query
        await search_input.click()
        await asyncio.sleep(0.5)
        
        # Clear any existing text
        await search_input.press("Control+A")
        await asyncio.sleep(0.2)
        await search_input.press("Delete")
        await asyncio.sleep(0.3)
        
        # Type company name with query
        query = f"{company_name} company website address linkedin"
        await search_input.type(query, delay=20)
        await asyncio.sleep(0.5)
        
        # Submit
        await search_input.press("Enter")
        logger.info(f"✓ Query submitted: {query}")
        await asyncio.sleep(3)
        
        # Wait for results
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except:
            pass
        
        # Wait for content to load
        await asyncio.sleep(10)
        
        # Take screenshot
        screenshot = await page.screenshot(type="png")
        
        # Get page text
        try:
            page_text = await page.inner_text("body")
        except:
            page_text = ""
        
        logger.info("✓ Search completed and screenshot captured")
        return screenshot, page_text
        
    except Exception as e:
        logger.error(f"Search error: {e}")
        print(f"✗ Search failed: {e}")
        return None, ""

# ============================================================================
# EXTRACT DATA FROM SINGLE SEARCH (NOT SEPARATED)
# ============================================================================

async def extract_all_company_data_fresh_v87(page, client, company_name):
    """Extract data from single fresh search result - SIMPLIFIED"""
    try:
        print(f"\n{'='*80}")
        print(f"PROCESSING: {company_name}")
        print(f"{'='*80}\n")
        
        results = {
            "company_name": company_name,
            "website": "NOT FOUND",
            "address": "NOT FOUND",
            "city": "NOT FOUND",
            "state": "NOT FOUND",
            "country": "NOT FOUND",
            "zipcode": "NOT FOUND",
            "email": "NOT FOUND",
            "phone": "NOT FOUND",
            "linkedin": "NOT FOUND",
            "industry": "NOT FOUND",
            "size": "NOT FOUND",
        }
        
        # SINGLE SEARCH: Navigate fresh and search for all data at once
        screenshot, text = await navigate_and_search_fresh(page, company_name, 1, client)
        
        if screenshot:
            # Save screenshot
            sspath = os.path.join(screenshot_dir, f"{company_name}_search.png")
            with open(sspath, "wb") as f:
                f.write(screenshot)
            logger.info(f"✓ Screenshot saved: {sspath}")
        else:
            sspath = None
        
        # Extract all fields from single screenshot
        if text or sspath:
            print("Extracting data...")
            
            # Website
            website = extract_website_v87(text, sspath, client, company_name)
            results["website"] = website
            if website != "NOT FOUND":
                print(f"✓ Website: {website}")
            
            # Address
            addr = extract_address_v87(text, sspath, client, company_name)
            results.update(addr)
            if addr["address"] != "NOT FOUND":
                print(f"✓ Address: {addr['address'][:60]}...")
            
            # LinkedIn
            linkedin = extract_linkedin_v87(text, sspath, client, company_name)
            results["linkedin"] = linkedin
            if linkedin != "NOT FOUND":
                print(f"✓ LinkedIn: {linkedin}")
            
            # Email
            email = extract_email_v87(text, sspath, client, company_name)
            results["email"] = email
            if email != "NOT FOUND":
                print(f"✓ Email: {email}")
            
            # Phone
            phone = extract_phone_v87(text, sspath, client, company_name)
            results["phone"] = phone
            if phone != "NOT FOUND":
                print(f"✓ Phone: {phone}")
            
            # Industry
            industry = extract_industry_v87(text, sspath, client, company_name)
            results["industry"] = industry
            if industry != "NOT FOUND":
                print(f"✓ Industry: {industry}")
            
            # Size
            size = extract_size_v87(text, sspath, client, company_name)
            results["size"] = size
            if size != "NOT FOUND":
                print(f"✓ Size: {size}")
        
        return results
    
    except Exception as e:
        logger.error(f"Extract error: {e}")
        print(f"✗ Extraction error: {e}")
        return results

# ============================================================================
# SHEET UPDATE
# ============================================================================

async def update_sheet_v87(sheet, row_num, results):
    """Update sheet with notes tracking"""
    try:
        logger.info(f"Updating sheet row {row_num}")
        print("→ Sheet: Updating...")
        
        notes = generate_notes_v87(results)
        
        update_pairs = [
            (1, results["company_name"]),
            (2, results["website"]),
            (3, results["address"]),
            (4, results["city"]),
            (5, results["state"]),
            (6, results["country"]),
            (7, results["zipcode"]),
            (8, results["email"]),
            (9, results["phone"]),
            (10, results["linkedin"]),
            (11, results["industry"]),
            (12, results["size"]),
            (13, "COMPLETED"),
            (14, datetime.datetime.now().strftime("%Y-%m-%d %H:%M")),
            (15, notes),
        ]
        
        for col, value in update_pairs:
            try:
                value_str = str(value) if value else "NOT FOUND"
                value_str = value_str.strip() if value_str else "NOT FOUND"
                if not value_str or value_str is None:
                    value_str = "NOT FOUND"
                sheet.update_cell(row_num, col, value_str)
            except Exception as e:
                logger.error(f"Cell update error at row {row_num}, col {col}: {e}")
        
        print(f"→ Sheet updated - Notes: {notes}")
        return True
        
    except Exception as e:
        logger.error(f"Update error: {e}")
        return False

# ============================================================================
# PROCESS COMPANY
# ============================================================================

async def process_company_fresh_v87(page, client, company_name, sheet, row_num):
    """Process company using fresh URL search"""
    try:
        results = await extract_all_company_data_fresh_v87(page, client, company_name)
        await update_sheet_v87(sheet, row_num, results)
        print(f"✓ {company_name} - COMPLETED\n")
        return True
    except Exception as e:
        logger.error(f"Process error: {e}")
        print(f"✗ {company_name} - FAILED: {e}\n")
        return False

# ============================================================================
# MAIN
# ============================================================================

async def main():
    """Main execution function"""
    logger.info(TITLE)
    logger.info("COMPANY RESEARCH v8.7 - FRESH URL NAVIGATION")
    logger.info(TITLE)
    
    print(TITLE)
    print("COMPANY RESEARCH v8.7 - FRESH URL NAVIGATION")
    print("✓ Fresh URL search for each company")
    print("✓ No follow-up chains (simpler & more reliable)")
    print("✓ Single search per company")
    print("✓ All data extracted from one result")
    print(TITLE)
    
    sheet, gc = setup_google_sheets_v87()
    if not sheet:
        print("ERROR: Could not connect to Google Sheets")
        return
    
    print("✓ OK - Sheet connected")
    
    client = get_gemini_client()
    if not client:
        print("ERROR: Gemini client failed")
        return
    
    print("✓ OK - Gemini API ready")
    
    try:
        all_data = sheet.get_all_values()
        companies = []
        
        for idx, row in enumerate(all_data[1:], start=2):
            company_name = row[0].strip() if len(row) > 0 else ""
            status = row[12].strip().upper() if len(row) > 12 else ""
            
            if company_name and (not status or status == "PENDING"):
                companies.append({"name": company_name, "row": idx})
        
        print(f"\nFound {len(companies)} companies to process")
        
        if not companies:
            print("No companies found with Status=PENDING")
            return
        
    except Exception as e:
        print(f"ERROR reading companies: {e}")
        logger.error(f"Error reading companies: {e}")
        return
    
    print("\nCompanies:")
    for i, c in enumerate(companies[:5], 1):
        print(f"{i}. {c['name']}")
    
    if len(companies) > 5:
        print(f"... {len(companies) - 5} more")
    
    confirm = input(f"\nProcess {len(companies)} companies? (Y/n): ").strip().lower()
    if confirm not in ["y", "yes", ""]:
        print("Processing cancelled.")
        return
    
    async with async_playwright() as p:
        print("\nLaunching browser...")
        browser = await p.chromium.launch(headless=False, slow_mo=30)
        context = await browser.new_context(
            viewport={"width": SCREEN_WIDTH, "height": SCREEN_HEIGHT}
        )
        page = await context.new_page()
        page.set_default_timeout(PAGE_LOAD_TIMEOUT)
        
        try:
            print("Starting processing...")
            successful = 0
            failed = 0
            
            for i, company_data in enumerate(companies, 1):
                print(f"\n[{i}/{len(companies)}]")
                result = await process_company_fresh_v87(
                    page, client, company_data["name"], sheet, company_data["row"]
                )
                if result:
                    successful += 1
                else:
                    failed += 1
                
                # Small delay between companies
                if i < len(companies):
                    await asyncio.sleep(2)
            
            print(TITLE)
            print(f"[{len(companies)}/{len(companies)}] COMPLETE")
            print(f"Successful: {successful}")
            print(f"Failed: {failed}")
            if len(companies) > 0:
                print(f"Success Rate: {(successful/len(companies))*100:.1f}%")
            print(f"\nLog: {log_filename}")
            print(f"Screenshots: {screenshot_dir}")
            print(TITLE)
            
        except Exception as e:
            logger.error(f"Error during processing: {e}")
            print(f"ERROR: {e}")
        finally:
            await browser.close()
            print("Browser closed.")

# ============================================================================
# EXECUTION
# ============================================================================

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nProcessing interrupted by user.")
        logger.info("Processing interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"FATAL: {e}")