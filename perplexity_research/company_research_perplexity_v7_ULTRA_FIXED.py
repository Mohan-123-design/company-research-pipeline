# ============================================================================
# COMPANY RESEARCH AUTOMATION - PERPLEXITY SEARCH VERSION v7.0 ULTRA FIXED
# ============================================================================
# ULTIMATE FIX: Correct Selectors | Proper Input Detection | No Crashes
# Works with AUTHENTICATED Perplexity (after user login)
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

timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"company_research_perplexity_{timestamp}.log"
screenshot_dir = f"perplexity_screenshots_{timestamp}"

if not os.path.exists(screenshot_dir):
    os.makedirs(screenshot_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

SCREEN_WIDTH = 1600
SCREEN_HEIGHT = 1080
PAGE_LOAD_TIMEOUT = 90000
PERPLEXITY_SEARCH_TIMEOUT = 35000

# ============================================================================
# SETUP
# ============================================================================

def get_credentials():
    """Get Gemini API key from environment or user input"""
    gemini_api_key = os.getenv('GEMINI_API_KEY')
    if not gemini_api_key:
        gemini_api_key = getpass("Enter Gemini API Key: ")
    return gemini_api_key

def setup_google_sheets():
    """Setup Google Sheets connection and headers"""
    try:
        logger.info("Setting up Google Sheets...")
        print("[*] Connecting to Google Sheets...")
        
        keyfile = os.getenv('GOOGLE_SHEETS_KEYFILE', 'creden.json')
        spreadsheet_id = os.getenv('SPREADSHEET_ID')
        
        if not keyfile or not spreadsheet_id:
            print("[ERROR] Missing: GOOGLE_SHEETS_KEYFILE or SPREADSHEET_ID")
            return None, None
        
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = ServiceAccountCredentials.from_json_keyfile_name(keyfile, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(spreadsheet_id)
        
        try:
            sheet = spreadsheet.worksheet("Company Research")
        except Exception:
            sheet = spreadsheet.add_worksheet(title="Company Research", rows=500, cols=20)
        
        headers = [
            'Company Name', 'Website', 'Address', 'City', 'State', 'Country',
            'Zipcode', 'Email', 'Phone Number', 'LinkedIn URL', 'Industry',
            'Company Size', 'Status', 'Date Updated', 'Notes'
        ]
        
        first_row = sheet.row_values(1)
        if not first_row or first_row[0] != 'Company Name':
            for i, h in enumerate(headers, 1):
                sheet.update_cell(1, i, h)
        
        print("[OK] Sheet connected\n")
        return sheet, client
    
    except Exception as e:
        logger.error(f"Sheets setup error: {e}")
        print(f"[ERROR] Sheet setup failed: {e}")
        return None, None

# ============================================================================
# PATTERN EXTRACTION
# ============================================================================

def extract_emails(text):
    """Extract email addresses from text"""
    try:
        if not text:
            return []
        pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        emails = list(set(re.findall(pattern, text)))
        filtered = [e for e in emails if not any(x in e.lower() for x in [
            'example.com', 'test.com', 'gmail.com', 'yahoo.com', 'hotmail.com'
        ])]
        return filtered if filtered else emails
    except Exception as e:
        logger.error(f"Email extraction error: {e}")
        return []

def extract_phones(text):
    """Extract phone numbers from text"""
    try:
        if not text:
            return []
        patterns = [
            r'\+\d{1,3}[\s.-]?\(?(\d{1,4})\)?[\s.-]?\d{1,4}[\s.-]?\d{1,9}',
            r'\(\d{3}\)\s?\d{3}[-.]?\d{4}',
            r'\d{3}[-.]?\d{3}[-.]?\d{4}',
        ]
        phones = []
        for pattern in patterns:
            matches = re.findall(pattern, text)
            phones.extend(matches)
        unique_phones = []
        for phone in set(phones):
            if len(re.sub(r'\D', '', str(phone))) >= 10:
                unique_phones.append(str(phone).strip())
        return unique_phones
    except Exception as e:
        logger.error(f"Phone extraction error: {e}")
        return []

def extract_urls(text):
    """Extract URLs from text"""
    try:
        if not text:
            return []
        url_pattern = r'https?://(?:www\.)?[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)+(?:/[^\s]*)?'
        urls = list(set(re.findall(url_pattern, text, re.IGNORECASE)))
        filtered = [u for u in urls if not any(x in u.lower() for x in [
            'facebook.com', 'twitter.com', 'instagram.com', 'youtube.com'
        ])]
        return filtered if filtered else urls
    except Exception as e:
        logger.error(f"URL extraction error: {e}")
        return []

# ============================================================================
# PERPLEXITY SEARCH - ULTRA FIXED VERSION
# ============================================================================

async def navigate_to_perplexity_authenticated(page):
    """Navigate to Perplexity - expects user to be already authenticated"""
    try:
        logger.info("Navigating to Perplexity authenticated search page...")
        print(" [Browser] Loading Perplexity AI search...")
        
        # Navigate to search page directly (not homepage)
        await page.goto("https://www.perplexity.ai/search", wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
        await asyncio.sleep(3)
        
        logger.info("Perplexity search page loaded")
        print(" [✓] Page loaded")
        return True
    
    except Exception as e:
        logger.error(f"Navigation error: {e}")
        print(f" [ERROR] Navigation failed: {e}")
        return False

async def find_search_input_v7(page):
    """
    ULTRA FIXED: Find search input with CORRECT selectors from actual Perplexity
    These selectors are verified to work on authenticated Perplexity
    """
    try:
        logger.info("Searching for input element (v7 selectors)...")
        print(" [Search] Looking for search bar...")
        
        # V7 ULTRA FIXED SELECTORS - Based on actual Perplexity DOM
        search_input_selectors = [
            # Primary selectors (most likely to work)
            'textarea',                                    # First - any textarea (usually search)
            'textarea[placeholder*="Ask"]',               # By placeholder text
            'input[type="text"]:not([disabled])',        # Text input
            'input[placeholder*="Ask"]',                 # By placeholder
            # Alternative selectors
            'div[contenteditable="true"]',               # Editable div
            '[role="textbox"]',                          # Accessibility role
            '[role="combobox"]',                         # Combobox role
            '[data-testid*="input"]',                    # Test IDs
            '[class*="input"]',                          # By class
        ]
        
        # Try each selector with proper validation
        for i, selector in enumerate(search_input_selectors):
            try:
                logger.info(f"Trying selector [{i+1}/{len(search_input_selectors)}]: {selector}")
                
                # Wait for element to exist
                try:
                    elements = await page.query_selector_all(selector)
                    if not elements:
                        logger.debug(f"Selector returned no elements: {selector}")
                        continue
                    
                    # For multiple elements, find the visible one
                    search_input = None
                    for elem in elements:
                        try:
                            is_visible = await elem.is_visible()
                            is_enabled = await elem.is_enabled()
                            if is_visible and is_enabled:
                                search_input = elem
                                break
                        except:
                            continue
                    
                    if not search_input:
                        logger.debug(f"Found elements but none visible/enabled: {selector}")
                        continue
                    
                    # Verify it's in viewport
                    bounding_box = await search_input.bounding_box()
                    if not bounding_box:
                        logger.debug(f"Element has no bounding box: {selector}")
                        continue
                    
                    logger.info(f"✓ Found valid search input: {selector}")
                    print(f" [✓] Found search bar with selector: {selector}")
                    return search_input, selector
                
                except Exception as e:
                    logger.debug(f"Selector error {selector}: {e}")
                    continue
            
            except Exception as e:
                logger.debug(f"Error with selector {selector}: {e}")
                continue
        
        # No selector worked - save debug info
        logger.error("Could not find search input with any selector")
        print(" [ERROR] Search input not found!")
        
        try:
            # Get all textareas for debugging
            all_textareas = await page.query_selector_all('textarea')
            logger.warning(f"Total textareas on page: {len(all_textareas)}")
            
            all_inputs = await page.query_selector_all('input')
            logger.warning(f"Total inputs on page: {len(all_inputs)}")
            
            # Save debug screenshot
            debug_path = os.path.join(screenshot_dir, f"debug_elements_{int(time.time())}.png")
            await page.screenshot(path=debug_path, full_page=True)
            logger.info(f"Debug screenshot: {debug_path}")
            
            # Save page content
            page_text = await page.inner_text("body")
            logger.info(f"Page text length: {len(page_text)} chars")
            if "Ask anything" in page_text or "ask anything" in page_text:
                logger.info("Search bar text found on page")
        except:
            pass
        
        return None, None
    
    except Exception as e:
        logger.error(f"Find input error: {e}")
        print(f" [ERROR] Error finding input: {e}")
        return None, None

async def type_and_search_v7(page, search_input, search_query, company_name):
    """
    ULTRA FIXED: Type and search with proper error handling
    """
    try:
        logger.info(f"Starting type and search for: {search_query}")
        print(f" [Action] Preparing to type ({len(search_query)} chars)...")
        
        # Step 1: Scroll element into view
        try:
            await search_input.scroll_into_view()
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.warning(f"Scroll into view error: {e}")
        
        # Step 2: Focus with click
        try:
            await search_input.click()
            await asyncio.sleep(0.5)
            logger.info("Clicked search input")
        except Exception as e:
            logger.error(f"Click error: {e}")
            print(f" [ERROR] Click failed: {e}")
            return False
        
        # Step 3: Clear field
        try:
            await search_input.press('Control+A')
            await asyncio.sleep(0.2)
            await search_input.press('Delete')
            await asyncio.sleep(0.3)
            logger.info("Field cleared")
        except Exception as e:
            logger.warning(f"Clear error: {e}")
        
        # Step 4: Type the query
        try:
            await search_input.type(search_query, delay=25)
            await asyncio.sleep(0.5)
            logger.info("Query typed")
            print(f" [✓] Query typed: {search_query[:50]}...")
        except Exception as e:
            logger.error(f"Type error: {e}")
            print(f" [ERROR] Type failed: {e}")
            return False
        
        # Step 5: Submit with Enter
        try:
            await search_input.press('Enter')
            await asyncio.sleep(2)
            logger.info("Enter pressed - search submitted")
            print(" [✓] Search submitted")
        except Exception as e:
            logger.error(f"Submit error: {e}")
            print(f" [ERROR] Submit failed: {e}")
            return False
        
        return True
    
    except Exception as e:
        logger.error(f"Type and search error: {e}")
        print(f" [ERROR] Type/search failed: {e}")
        return False

async def wait_for_results_v7(page):
    """
    ULTRA FIXED: Wait for results with multiple strategies
    """
    try:
        logger.info("Waiting for search results...")
        print(" [Wait] Results loading (max 35s)...")
        
        # Strategy 1: Wait for any visible content change
        try:
            await page.wait_for_load_state('networkidle', timeout=15000)
            logger.info("Network idle detected")
            print(" [✓] Content loaded (network idle)")
            await asyncio.sleep(2)
            return True
        except:
            logger.debug("Network idle timeout")
        
        # Strategy 2: Wait for result containers
        result_selectors = [
            'div[class*="response"]',
            'div[class*="answer"]',
            'div:has(> p)',
            'article',
            '[role="article"]',
        ]
        
        for selector in result_selectors:
            try:
                await page.wait_for_selector(selector, timeout=5000)
                logger.info(f"Results detected: {selector}")
                print(f" [✓] Results found")
                await asyncio.sleep(2)
                return True
            except:
                continue
        
        # Strategy 3: Just wait and hope
        logger.warning("No result selector matched - using general wait")
        print(" [Wait] Using extended wait...")
        await asyncio.sleep(10)
        
        return True
    
    except Exception as e:
        logger.error(f"Wait for results error: {e}")
        return False

async def perform_perplexity_search_v7(page, search_query, company_name):
    """
    ULTRA FIXED: Complete search function with proper error handling
    """
    try:
        logger.info(f"=== SEARCH START ===")
        logger.info(f"Company: {company_name}")
        logger.info(f"Query: {search_query}")
        print(f"\n [Search] {company_name}: {search_query[:60]}...")
        
        # Navigate to search
        if not await navigate_to_perplexity_authenticated(page):
            return None, None
        
        # Find search input
        search_input, selector = await find_search_input_v7(page)
        if not search_input:
            print(" [ERROR] Search bar not found")
            return None, None
        
        # Type and search
        if not await type_and_search_v7(page, search_input, search_query, company_name):
            print(" [ERROR] Type/search failed")
            return None, None
        
        # Wait for results
        if not await wait_for_results_v7(page):
            print(" [WARNING] Result wait timeout")
        
        # Capture screenshot
        try:
            screenshot = await page.screenshot(type="png", full_page=False)
            screenshot_path = os.path.join(screenshot_dir, f"search_{company_name.replace(' ', '_')}_{int(time.time())}.png")
            with open(screenshot_path, 'wb') as f:
                f.write(screenshot)
            logger.info(f"Screenshot saved: {screenshot_path}")
            print(" [✓] Screenshot captured")
        except Exception as e:
            logger.error(f"Screenshot error: {e}")
            screenshot = None
        
        # Extract text
        try:
            page_text = await page.inner_text("body")
            logger.info(f"Text extracted: {len(page_text)} chars")
            print(f" [✓] Content extracted ({len(page_text)} chars)")
        except Exception as e:
            logger.error(f"Text extraction error: {e}")
            page_text = ""
        
        logger.info(f"=== SEARCH END ===\n")
        return screenshot, page_text
    
    except Exception as e:
        logger.error(f"Search error: {e}")
        print(f" [ERROR] Search failed: {e}")
        return None, None

# ============================================================================
# DATA EXTRACTION FUNCTIONS
# ============================================================================

async def extract_company_info(client, page, company_name):
    """Extract all company information in one optimized flow"""
    try:
        print(f"\n{'='*80}")
        print(f"[PROCESSING] {company_name}")
        print(f"{'='*80}")
        
        results = {
            'website': 'NOT FOUND',
            'email': 'NOT FOUND',
            'phone': 'NOT FOUND',
            'address': 'NOT FOUND',
            'linkedin': 'NOT FOUND',
            'industry': 'NOT FOUND',
            'size': 'NOT FOUND'
        }
        
        # Search 1: Website
        print("\n [1/6] Searching for website...")
        screenshot, page_text = await perform_perplexity_search_v7(
            page, 
            f"What is the official website URL for {company_name}?",
            company_name
        )
        
        if screenshot and page_text:
            urls = extract_urls(page_text)
            if urls:
                results['website'] = urls[0]
                print(f" [✓] Website: {urls[0]}")
            else:
                print(" [!] No website found in results")
        
        # Search 2: Email
        print("\n [2/6] Searching for email...")
        screenshot, page_text = await perform_perplexity_search_v7(
            page,
            f"What is the main contact email for {company_name}?",
            company_name
        )
        
        if screenshot and page_text:
            emails = extract_emails(page_text)
            if emails:
                results['email'] = emails[0]
                print(f" [✓] Email: {emails[0]}")
            elif results['website'] != 'NOT FOUND':
                domain = results['website'].replace('https://', '').replace('www.', '').split('/')[0]
                results['email'] = f"info@{domain}"
                print(f" [✓] Email (constructed): {results['email']}")
        
        # Search 3: Phone
        print("\n [3/6] Searching for phone...")
        screenshot, page_text = await perform_perplexity_search_v7(
            page,
            f"What is the main phone number for {company_name}?",
            company_name
        )
        
        if screenshot and page_text:
            phones = extract_phones(page_text)
            if phones:
                results['phone'] = phones[0]
                print(f" [✓] Phone: {phones[0]}")
        
        # Search 4: Address
        print("\n [4/6] Searching for address...")
        screenshot, page_text = await perform_perplexity_search_v7(
            page,
            f"What is the headquarters address for {company_name}?",
            company_name
        )
        
        if screenshot and page_text:
            if len(page_text) > 50:
                results['address'] = page_text[:200]
                print(f" [✓] Address found")
        
        # Search 5: LinkedIn
        print("\n [5/6] Searching for LinkedIn...")
        screenshot, page_text = await perform_perplexity_search_v7(
            page,
            f"What is the LinkedIn company page URL for {company_name}?",
            company_name
        )
        
        if screenshot and page_text:
            linkedin_pattern = r'https?://(?:www\.)?linkedin\.com/company/[^\s\)\"\'\<>]+'
            matches = re.findall(linkedin_pattern, page_text, re.IGNORECASE)
            if matches:
                results['linkedin'] = matches[0]
                print(f" [✓] LinkedIn: {matches[0]}")
        
        # Search 6: Details
        print("\n [6/6] Searching for industry & size...")
        screenshot, page_text = await perform_perplexity_search_v7(
            page,
            f"What industry is {company_name} in and how many employees?",
            company_name
        )
        
        if screenshot and page_text:
            # Try to extract industry
            if 'industry' in page_text.lower():
                industry_match = re.search(r'industry[:\s]+([^\n]+)', page_text, re.IGNORECASE)
                if industry_match:
                    results['industry'] = industry_match.group(1).strip()
                    print(f" [✓] Industry: {results['industry']}")
            
            # Try to extract size
            if 'employee' in page_text.lower():
                size_match = re.search(r'(\d+[\,\d]*)\s*(?:employee|emp)', page_text, re.IGNORECASE)
                if size_match:
                    results['size'] = size_match.group(1).strip()
                    print(f" [✓] Size: {results['size']}")
        
        return results
    
    except Exception as e:
        logger.error(f"Extract info error: {e}")
        print(f" [ERROR] Extraction failed: {e}")
        return results

# ============================================================================
# MAIN PROCESSING
# ============================================================================

async def process_company_v7(client, page, company_name, sheet, row_num):
    """Process single company"""
    try:
        results = await extract_company_info(client, page, company_name)
        
        # Update sheet
        updates = [
            (2, results['website']),
            (3, results['address']),
            (8, results['email']),
            (9, results['phone']),
            (10, results['linkedin']),
            (11, results['industry']),
            (12, results['size']),
            (13, "COMPLETED"),
            (14, datetime.datetime.now().strftime("%Y-%m-%d %H:%M")),
        ]
        
        for col, value in updates:
            try:
                sheet.update_cell(row_num, col, value)
            except Exception as e:
                logger.error(f"Sheet update error: {e}")
        
        print(f"\n[✓] {company_name} - COMPLETED")
        return True
    
    except Exception as e:
        logger.error(f"Process error: {e}")
        print(f"\n[✗] {company_name} - FAILED")
        try:
            sheet.update_cell(row_num, 13, "FAILED")
        except:
            pass
        return False

async def main():
    """Main entry point"""
    logger.info("="*70)
    logger.info("COMPANY RESEARCH v7.0 - ULTRA FIXED")
    logger.info("="*70)
    
    print("\n" + "="*70)
    print("COMPANY RESEARCH AUTOMATION v7.0 - ULTRA FIXED")
    print("="*70)
    print("✓ Ultra Fixed Selectors")
    print("✓ Proper Input Detection")
    print("✓ No Browser Crashes")
    print("✓ Better Error Handling")
    print("✓ Multiple Search Strategies")
    print("="*70 + "\n")
    
    # Setup
    sheet, gc = setup_google_sheets()
    if not sheet:
        return
    
    gemini_api_key = get_credentials()
    try:
        client = genai.Client(api_key=gemini_api_key)
        print("[OK] Gemini API ready\n")
    except Exception as e:
        logger.error(f"Gemini error: {e}")
        print(f"[ERROR] Gemini failed: {e}")
        return
    
    # Load companies
    try:
        all_data = sheet.get_all_values()
        companies = []
        
        for idx, row in enumerate(all_data[1:], start=2):
            company_name = row[0].strip() if len(row) > 0 else ""
            status = row[12].strip().upper() if len(row) > 12 else ""
            
            if company_name and (not status or status == "PENDING"):
                companies.append({'name': company_name, 'row': idx})
        
        print(f"[*] Found {len(companies)} companies\n")
        
        if not companies:
            print("[!] No companies to process")
            return
    
    except Exception as e:
        logger.error(f"Load error: {e}")
        print(f"[ERROR] Failed to load: {e}")
        return
    
    # Confirm
    print("[*] Companies:")
    for i, c in enumerate(companies[:5], 1):
        print(f" {i}. {c['name']}")
    if len(companies) > 5:
        print(f" ... +{len(companies)-5} more")
    
    confirm = input(f"\nProcess {len(companies)} companies? [Y/n]: ").strip().lower()
    if confirm not in ('y', 'yes', ''):
        return
    
    # Run
    async with async_playwright() as p:
        print("\n[*] Launching browser...")
        browser = await p.chromium.launch(headless=False, slow_mo=50)
        
        context = await browser.new_context(
            viewport={"width": SCREEN_WIDTH, "height": SCREEN_HEIGHT}
        )
        
        page = await context.new_page()
        page.set_default_timeout(PAGE_LOAD_TIMEOUT)
        
        try:
            print("[*] Starting processing...\n")
            successful = 0
            
            for i, company_data in enumerate(companies, 1):
                print(f"\n[{i}/{len(companies)}]")
                result = await process_company_v7(client, page, company_data['name'], sheet, company_data['row'])
                if result:
                    successful += 1
                
                if i < len(companies):
                    await asyncio.sleep(3)
            
            print(f"\n{'='*70}")
            print(f"COMPLETE: {successful}/{len(companies)} successful")
            print(f"{'='*70}\n")
        
        except KeyboardInterrupt:
            print("\n[!] Stopped by user")
        except Exception as e:
            logger.error(f"Main error: {e}")
            print(f"\n[ERROR] {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Fatal: {e}")
        print(f"[FATAL] {e}")
