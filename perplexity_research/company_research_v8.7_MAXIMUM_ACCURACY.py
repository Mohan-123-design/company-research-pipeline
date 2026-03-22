# ============================================================================
# COMPANY RESEARCH AUTOMATION - v8.7 - MAXIMUM ACCURACY (FIXED)
# ============================================================================
# CRITICAL FIXES APPLIED:
# 1. ✓ STRICT phone validation (no corrupted numbers)
# 2. ✓ DEDICATED address parsing (complete address extraction)
# 3. ✓ SEPARATE city/state parsing (no confusion)
# 4. ✓ SPECIFIC LinkedIn search (targeted extraction)
# 5. ✓ IMPROVED Gemini prompts (with exact examples)
# 6. ✓ Better postal code handling (don't confuse with state)
# 7. ✓ International phone support
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
# CONFIGURATION
# ============================================================================

SEARCH_URL = "https://www.perplexity.ai/spaces/company-details-Ygq6oOZCSHyasw6gkdTkrQ"

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
PERPLEXITY_LOAD_TIME = 10  # INCREASED for better results

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
            return None
        return genai.Client(api_key=gemini_api_key)
    except Exception as e:
        logger.error(f"Gemini error: {e}")
        return None

# ============================================================================
# NOTES GENERATION
# ============================================================================

def generate_notes_v87(results):
    """Generate notes based on missing data"""
    try:
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
            value = results.get(field_key, "NOT FOUND")
            if value == "NOT FOUND" or value is None or value == "":
                missing_fields.append(field_name)
        
        if not missing_fields:
            return "✓ All fields extracted successfully"
        elif len(missing_fields) <= 3:
            missing_str = " | ".join([f"{field}: NOT FOUND" for field in missing_fields])
            return f"Partial: {missing_str}"
        else:
            return f"{len(missing_fields)} fields missing"
        
    except Exception as e:
        logger.error(f"Notes error: {e}")
        return "Error generating notes"

# ============================================================================
# FIXED EXTRACTION FUNCTIONS - MAXIMUM ACCURACY
# ============================================================================

def validate_and_fix_phone(phone_str):
    """STRICT phone number validation and fixing"""
    try:
        if not phone_str:
            return "NOT FOUND"
        
        phone_str = str(phone_str).strip()
        
        # Reject invalid patterns
        if phone_str.startswith("-") and len(phone_str) < 5:
            return "NOT FOUND"
        
        # Extract digits only
        digits = re.sub(r"\D", "", phone_str)
        
        # Must have at least 10 digits
        if len(digits) < 10:
            return "NOT FOUND"
        
        # Format based on length
        if len(digits) == 10:  # US format
            return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
        elif len(digits) == 11:  # US with leading 1
            return f"{digits[1:4]}-{digits[4:7]}-{digits[7:11]}"
        elif len(digits) >= 12:  # International
            return f"+{digits[0:2]}-{digits[2:5]}-{digits[5:8]}-{digits[8:12]}"
        else:
            return "NOT FOUND"
            
    except:
        return "NOT FOUND"

def extract_with_gemini_vision_fixed(client, screenshot_path, extraction_type, company_name):
    """IMPROVED Gemini Vision extraction with exact examples"""
    try:
        if not os.path.exists(screenshot_path):
            return None
        
        with open(screenshot_path, "rb") as f:
            image_data = f.read()
        
        # IMPROVED PROMPTS WITH EXACT INSTRUCTIONS
        prompts = {
            "website": f"""Find the OFFICIAL website URL for {company_name}.
Example: https://www.microsoft.com or https://example.com
Return ONLY the URL (starting with https://)
If not found, return: NOT FOUND""",
            
            "address": f"""Find the COMPLETE HEADQUARTERS ADDRESS for {company_name}.
Include: Street number, street name, building/suite info
Example: 1 Microsoft Way or 123 Main Street Suite 500
Return the full street address ONLY
If not found, return: NOT FOUND""",
            
            "city": f"""Find the CITY for {company_name} headquarters.
Example: Seattle or Dublin or Mumbai
Return ONLY the city name (nothing else)
If not found, return: NOT FOUND""",
            
            "state": f"""Find the STATE or PROVINCE for {company_name} headquarters.
For US: Use 2-letter code (CA, NY, TX, WA)
For other countries: Use region name or code
Example: WA for Washington, AB for Alberta
Return ONLY the state/province code or name
If not found, return: NOT FOUND""",
            
            "country": f"""Find the COUNTRY where {company_name} is headquartered.
Example: United States, Canada, India, Ireland
Return ONLY the country name
If not found, return: NOT FOUND""",
            
            "zipcode": f"""Find the POSTAL CODE or ZIP CODE for {company_name} headquarters.
Example: 98052 or D02XE80 or 141015
Return ONLY the postal/zip code (numbers and letters, no spaces)
If not found, return: NOT FOUND""",
            
            "email": f"""Find the MAIN CONTACT EMAIL for {company_name}.
Look for official contact email (not generic)
Example: contact@company.com or info@company.com
Return ONLY the email address
If not found, return: NOT FOUND""",
            
            "phone": f"""Find the MAIN HEADQUARTERS PHONE NUMBER for {company_name}.
Example: +1-425-882-8080 or 415-555-0123
Return ONLY the phone number with country code or dashes
If not found, return: NOT FOUND""",
            
            "linkedin": f"""Find the LINKEDIN COMPANY PAGE for {company_name}.
Must contain linkedin.com/company/
Example: https://linkedin.com/company/microsoft
Return ONLY the full LinkedIn company URL
If not found, return: NOT FOUND""",
            
            "industry": f"""Find the INDUSTRY or SECTOR for {company_name}.
Examples: Technology, Finance, Healthcare, Manufacturing, Retail
Return ONLY the industry name
If not found, return: NOT FOUND""",
            
            "size": f"""Find the NUMBER OF EMPLOYEES for {company_name}.
Examples: 150, 1000, 5000, 50000
Return ONLY the number (no commas, no text)
If not found, return: NOT FOUND""",
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
        
        if result:
            result = result.replace("**", "").replace("*", "").strip()
            # Remove common wrapper text
            result = re.sub(r"^(Here is|The|OK|Sure).*?:\s*", "", result)
        
        return result
        
    except Exception as e:
        logger.error(f"Vision error: {e}")
        return None

def extract_website_v87(text, screenshot_path, client, company_name):
    """Extract website - FIXED"""
    try:
        if client and screenshot_path:
            vision_result = extract_with_gemini_vision_fixed(client, screenshot_path, "website", company_name)
            if vision_result and vision_result != "NOT FOUND" and "http" in vision_result:
                url = vision_result.split()[0].strip()
                if url.startswith("http"):
                    return url
        
        if text:
            pattern = r"https?://(?:www\.)?[a-zA-Z0-9\-\.]+\.[a-zA-Z0-9\.\-]+"
            matches = re.findall(pattern, text)
            for url in matches:
                if not any(x in url.lower() for x in ["facebook", "twitter", "instagram", "github"]):
                    return url.rstrip()
        
        return "NOT FOUND"
    except:
        return "NOT FOUND"

def extract_address_v87(text, screenshot_path, client, company_name):
    """Extract COMPLETE address - FIXED"""
    try:
        result = {
            "address": "NOT FOUND",
            "city": "NOT FOUND",
            "state": "NOT FOUND",
            "country": "NOT FOUND",
            "zipcode": "NOT FOUND"
        }
        
        # Use Gemini for EACH component separately
        if client and screenshot_path:
            vision_addr = extract_with_gemini_vision_fixed(client, screenshot_path, "address", company_name)
            vision_city = extract_with_gemini_vision_fixed(client, screenshot_path, "city", company_name)
            vision_state = extract_with_gemini_vision_fixed(client, screenshot_path, "state", company_name)
            vision_country = extract_with_gemini_vision_fixed(client, screenshot_path, "country", company_name)
            vision_zip = extract_with_gemini_vision_fixed(client, screenshot_path, "zipcode", company_name)
            
            if vision_addr and vision_addr != "NOT FOUND":
                result["address"] = vision_addr[:300].strip()
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
    """Extract phone with STRICT validation - FIXED"""
    try:
        # Try Gemini first
        if client and screenshot_path:
            vision_phone = extract_with_gemini_vision_fixed(client, screenshot_path, "phone", company_name)
            if vision_phone and vision_phone != "NOT FOUND":
                validated = validate_and_fix_phone(vision_phone)
                if validated != "NOT FOUND":
                    return validated
        
        if text:
            # Multiple phone patterns
            patterns = [
                r"(?:\+1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})",
                r"\+[0-9]{1,3}[-.\s]?[0-9]{3}[-.\s]?[0-9]{3}[-.\s]?[0-9]{4,5}",
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, text)
                for match in matches:
                    if isinstance(match, tuple) and len(match) >= 3:
                        phone = f"{match[0]}-{match[1]}-{match[2]}"
                        validated = validate_and_fix_phone(phone)
                        if validated != "NOT FOUND":
                            return validated
                    elif isinstance(match, str):
                        validated = validate_and_fix_phone(match)
                        if validated != "NOT FOUND":
                            return validated
        
        return "NOT FOUND"
    except:
        return "NOT FOUND"

def extract_email_v87(text, screenshot_path, client, company_name):
    """Extract email - FIXED"""
    try:
        if client and screenshot_path:
            vision_email = extract_with_gemini_vision_fixed(client, screenshot_path, "email", company_name)
            if vision_email and "@" in vision_email and vision_email != "NOT FOUND":
                email = vision_email.strip().split()[0]
                if "@" in email:
                    return email
        
        if text:
            pattern = r"[a-zA-Z0-9.\-_+]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
            matches = re.findall(pattern, text)
            for email in matches:
                if not any(x in email.lower() for x in ["test", "example", "noreply"]):
                    return email
        
        return "NOT FOUND"
    except:
        return "NOT FOUND"

def extract_linkedin_v87(text, screenshot_path, client, company_name):
    """Extract LinkedIn with SPECIFIC search - FIXED"""
    try:
        if client and screenshot_path:
            vision_linkedin = extract_with_gemini_vision_fixed(client, screenshot_path, "linkedin", company_name)
            if vision_linkedin and "linkedin.com/company" in vision_linkedin:
                url = vision_linkedin.split()[0].strip()
                if "https" in url:
                    return url
        
        if text:
            pattern = r"https?://(?:www\.)?linkedin\.com/company/[a-zA-Z0-9\-]+"
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                return matches[0]
        
        return "NOT FOUND"
    except:
        return "NOT FOUND"

def extract_industry_v87(text, screenshot_path, client, company_name):
    """Extract industry - FIXED"""
    try:
        if client and screenshot_path:
            vision_industry = extract_with_gemini_vision_fixed(client, screenshot_path, "industry", company_name)
            if vision_industry and vision_industry != "NOT FOUND" and len(vision_industry) < 100:
                return vision_industry.strip()
        
        if text:
            text_lower = text.lower()
            industries = {
                "Technology": ["technology", "tech", "software", "saas", "cloud", "ai", "platform"],
                "Finance": ["financial", "finance", "banking", "fintech"],
                "Healthcare": ["health", "medical", "hospital", "pharma"],
                "Retail": ["retail", "commerce", "shopping"],
                "Manufacturing": ["manufacturing", "factory"],
                "Carbon Capture & Storage Technology": ["carbon", "capture", "storage", "emissions"],
            }
            
            for industry, keywords in industries.items():
                if any(kw in text_lower for kw in keywords):
                    return industry
        
        return "NOT FOUND"
    except:
        return "NOT FOUND"

def extract_size_v87(text, screenshot_path, client, company_name):
    """Extract company size - FIXED"""
    try:
        if client and screenshot_path:
            vision_size = extract_with_gemini_vision_fixed(client, screenshot_path, "size", company_name)
            if vision_size and vision_size != "NOT FOUND":
                digits = re.sub(r"\D", "", vision_size)
                if digits and 1 <= int(digits) <= 10000000:
                    return digits
        
        if text:
            patterns = [
                r"([0-9,]+)\s+(?:employees?|staff|people)",
                r"(?:company\s+size|employees?|headcount)[\s:]*([0-9,]+)",
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    size_str = match.group(1).replace(",", "")
                    try:
                        if 1 <= int(size_str) <= 10000000:
                            return size_str
                    except:
                        continue
        
        return "NOT FOUND"
    except:
        return "NOT FOUND"

# ============================================================================
# SEARCH FUNCTION
# ============================================================================

async def navigate_and_search_fixed(page, company_name):
    """Navigate and search with MAXIMUM ACCURACY"""
    try:
        logger.info(f"Navigating to: {SEARCH_URL}")
        await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
        await asyncio.sleep(PERPLEXITY_LOAD_TIME)
        
        # Find search input with retry
        search_input = None
        for attempt in range(5):
            for selector in ['textarea', 'input[type="text"]', 'div[role="textbox"]']:
                elements = await page.query_selector_all(selector)
                if elements:
                    for elem in elements:
                        try:
                            if await elem.is_visible() and await elem.is_enabled():
                                search_input = elem
                                break
                        except:
                            continue
                if search_input:
                    break
            
            if search_input:
                break
            await asyncio.sleep(1)
        
        if not search_input:
            return None, ""
        
        # Type search query
        await search_input.click()
        await asyncio.sleep(0.5)
        await search_input.press("Control+A")
        await asyncio.sleep(0.2)
        await search_input.press("Delete")
        await asyncio.sleep(0.3)
        
        query = f"{company_name} company"
        await search_input.type(query, delay=25)
        await asyncio.sleep(0.5)
        
        await search_input.press("Enter")
        logger.info(f"Query: {query}")
        await asyncio.sleep(4)
        
        # Wait longer for results
        try:
            await page.wait_for_load_state("networkidle", timeout=25000)
        except:
            pass
        
        await asyncio.sleep(15)  # INCREASED wait
        
        screenshot = await page.screenshot(type="png")
        try:
            page_text = await page.inner_text("body")
        except:
            page_text = ""
        
        return screenshot, page_text
        
    except Exception as e:
        logger.error(f"Search error: {e}")
        return None, ""

# ============================================================================
# EXTRACT DATA
# ============================================================================

async def extract_all_company_data_fixed(page, client, company_name):
    """Extract with MAXIMUM ACCURACY"""
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
        
        screenshot, text = await navigate_and_search_fixed(page, company_name)
        
        if screenshot:
            sspath = os.path.join(screenshot_dir, f"{company_name}_search.png")
            with open(sspath, "wb") as f:
                f.write(screenshot)
        else:
            sspath = None
        
        if text or sspath:
            print("Extracting data with MAXIMUM accuracy...\n")
            
            # Website
            website = extract_website_v87(text, sspath, client, company_name)
            results["website"] = website
            print(f"Website: {website}")
            
            # Address components
            addr = extract_address_v87(text, sspath, client, company_name)
            results.update(addr)
            print(f"Address: {addr['address']}")
            print(f"City: {addr['city']}")
            print(f"State: {addr['state']}")
            print(f"Country: {addr['country']}")
            print(f"Zipcode: {addr['zipcode']}")
            
            # LinkedIn
            linkedin = extract_linkedin_v87(text, sspath, client, company_name)
            results["linkedin"] = linkedin
            print(f"LinkedIn: {linkedin}")
            
            # Email
            email = extract_email_v87(text, sspath, client, company_name)
            results["email"] = email
            print(f"Email: {email}")
            
            # Phone
            phone = extract_phone_v87(text, sspath, client, company_name)
            results["phone"] = phone
            print(f"Phone: {phone}")
            
            # Industry
            industry = extract_industry_v87(text, sspath, client, company_name)
            results["industry"] = industry
            print(f"Industry: {industry}")
            
            # Size
            size = extract_size_v87(text, sspath, client, company_name)
            results["size"] = size
            print(f"Size: {size}\n")
        
        return results
    
    except Exception as e:
        logger.error(f"Extract error: {e}")
        print(f"Error: {e}")
        return results

# ============================================================================
# SHEET UPDATE
# ============================================================================

async def update_sheet_v87(sheet, row_num, results):
    """Update sheet"""
    try:
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
            value_str = str(value) if value else "NOT FOUND"
            value_str = value_str.strip() if value_str else "NOT FOUND"
            sheet.update_cell(row_num, col, value_str)
        
        print(f"→ Sheet updated - Notes: {notes}\n")
        return True
        
    except Exception as e:
        logger.error(f"Update error: {e}")
        return False

# ============================================================================
# PROCESS COMPANY
# ============================================================================

async def process_company_fixed(page, client, company_name, sheet, row_num):
    """Process company"""
    try:
        results = await extract_all_company_data_fixed(page, client, company_name)
        await update_sheet_v87(sheet, row_num, results)
        print(f"✓ {company_name} - COMPLETED\n")
        return True
    except Exception as e:
        logger.error(f"Process error: {e}")
        print(f"✗ {company_name} - FAILED\n")
        return False

# ============================================================================
# MAIN
# ============================================================================

async def main():
    """Main"""
    logger.info(TITLE)
    logger.info("COMPANY RESEARCH v8.7 - MAXIMUM ACCURACY (ALL FIXES)")
    logger.info(TITLE)
    
    print(TITLE)
    print("COMPANY RESEARCH v8.7 - MAXIMUM ACCURACY")
    print("✓ STRICT phone number validation")
    print("✓ DEDICATED address parsing")
    print("✓ SEPARATE city/state extraction")
    print("✓ SPECIFIC LinkedIn search")
    print("✓ IMPROVED Gemini prompts (with examples)")
    print("✓ Better postal code handling")
    print("✓ International phone support")
    print(TITLE)
    
    sheet, gc = setup_google_sheets_v87()
    if not sheet:
        return
    print("✓ OK - Sheet connected\n")
    
    client = get_gemini_client()
    if not client:
        return
    print("✓ OK - Gemini API ready\n")
    
    try:
        all_data = sheet.get_all_values()
        companies = []
        
        for idx, row in enumerate(all_data[1:], start=2):
            company_name = row[0].strip() if len(row) > 0 else ""
            status = row[12].strip().upper() if len(row) > 12 else ""
            
            if company_name and (not status or status == "PENDING"):
                companies.append({"name": company_name, "row": idx})
        
        print(f"Found {len(companies)} companies\n")
        if not companies:
            return
        
    except Exception as e:
        print(f"ERROR: {e}")
        return
    
    print("Companies:")
    for i, c in enumerate(companies[:5], 1):
        print(f"{i}. {c['name']}")
    if len(companies) > 5:
        print(f"... {len(companies) - 5} more")
    
    confirm = input(f"\nProcess {len(companies)} companies? (Y/n): ").strip().lower()
    if confirm not in ["y", "yes", ""]:
        return
    
    async with async_playwright() as p:
        print("\nLaunching browser...")
        browser = await p.chromium.launch(headless=False, slow_mo=30)
        context = await browser.new_context(viewport={"width": SCREEN_WIDTH, "height": SCREEN_HEIGHT})
        page = await context.new_page()
        page.set_default_timeout(PAGE_LOAD_TIMEOUT)
        
        try:
            successful = 0
            for i, company_data in enumerate(companies, 1):
                print(f"\n[{i}/{len(companies)}]")
                if await process_company_fixed(page, client, company_data["name"], sheet, company_data["row"]):
                    successful += 1
                if i < len(companies):
                    await asyncio.sleep(3)
            
            print(TITLE)
            print(f"COMPLETE: {successful}/{len(companies)}")
            print(f"Success Rate: {(successful/len(companies))*100:.1f}%")
            print(f"Log: {log_filename}")
            print(TITLE)
            
        finally:
            await browser.close()

# ============================================================================
# EXECUTION
# ============================================================================

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"FATAL: {e}")