from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import csv
import time
import os
from datetime import datetime, timedelta
import re
import signal
import sys
import traceback
import argparse
import json
import hashlib

# Add this near the top of your file, after imports:
import platform

# Modify the initialize_driver function to handle Docker better:
def initialize_driver(headless=True):
    """
    Initialize Chrome driver optimized for headless operation
    """
    chrome_options = Options()
    
    # Essential options for headless operation
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Additional Docker-specific optimizations
    if os.environ.get('DOCKER_MODE') == 'true':
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--remote-debugging-port=9222")
        chrome_options.add_argument("--remote-debugging-address=0.0.0.0")
    
    # [Rest of the function remains the same...]
#fix this to only use the yml file directory path for better scalability!!
# Use Docker-specific paths
if os.environ.get('DOCKER_MODE') == 'true':
    DEFAULT_OUTPUT_DIR = "/home/scraper/data"

current_output_file = None
current_settings_hash = None

# ------------------------------
# Mapping: currency codes -> exact labels (as shown in the UI)
# ------------------------------
CURRENCY_LABELS = {
    "AUD": "AUD - Australian Dollar",
    "BRL": "BRL - Brazilian real",
    "CAD": "CAD - Canadian dollar",
    "CHF": "CHF - Swiss frank",
    "CNY": "CNY - Chinese yuan",
    "EUR": "EUR - Euro",
    "GBP": "GBP - Pound sterling",
    "HKD": "HKD - Hong Kong dollar",
    "INR": "INR - Indian rupee",
    "JPY": "JPY - Japanese yen",
    "KRW": "KRW - South Korean won",
    "MXN": "MXN - Mexican peso",
    "NOK": "NOK - Norwegian Krone",
    "NZD": "NZD - New Zealand dollar",
    "SEK": "SEK - Swedish krona",
    "SGD": "SGD - Singapore dollar",
    "USD": "USD - US dollar",
    "ZAR": "ZAR - South African rand"
}

IMPORTANCE_LABELS = {
    "High": "High",
    "Medium": "Medium",
    "Low": "Low",
    "Holidays": "Holidays"
}

# Global variables
driver = None
wait = None
shutdown_flag = False
graceful_exit = False
first_week_collected = None
last_week_collected = None
settings_hash = None
resuming_from_state = False

def get_previous_month_name():
    """Get the name of the previous month in 'Month Year' format"""
    today = datetime.now()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    return last_day_of_previous_month.strftime("%B %Y")

def signal_handler(sig, frame):
    global shutdown_flag, graceful_exit
    print('\nShutdown signal received! Saving collected data...')
    shutdown_flag = True
    graceful_exit = True

def get_current_week_range():
    try:
        current_week_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//div[@id='economicCalendarTable']//div[contains(@class, 'ec-table__nav__item_current')]")
            )
        )
        return current_week_element.text.strip()
    except Exception as e:
        print(f"Error getting week range: {str(e)}")
        return None

def set_to_current_week():
    """Click the 'Current week' button to set the calendar to the current week"""
    try:
        current_week_label_xpath = "//label[@for='filterDate1' and contains(., 'Current week')]"
        current_week_label = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, current_week_label_xpath))
        )
        
        parent_li = driver.find_element(By.XPATH, "//label[@for='filterDate1']/..")
        if "active" not in parent_li.get_attribute("class"):
            print("Clicking 'Current week' label...")
            safe_click(current_week_label)
            time.sleep(3)
        else:
            print("Calendar is already set to current week.")
            
        return True
    except Exception as e:
        print(f"Error setting to current week: {e}")
        try:
            current_week_selector = "//input[@id='filterDate1']"
            current_week_radio = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, current_week_selector))
            )
            if not current_week_radio.is_selected():
                print("Fallback: Clicking 'Current week' radio button...")
                safe_click(current_week_radio)
                time.sleep(3)
            return True
        except Exception as e2:
            print(f"Fallback also failed: {e2}")
            return False

def set_to_previous_month():
    """Click the 'Previous month' button to set the calendar to the previous month"""
    try:
        time.sleep(2)
        
        previous_month_label_xpath = "//label[@for='filterDate5' and contains(., 'Previous month')]"
        previous_month_label = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, previous_month_label_xpath))
        )
        
        parent_li = driver.find_element(By.XPATH, "//label[@for='filterDate5']/..")
        if "active" not in parent_li.get_attribute("class"):
            print("Clicking 'Previous month' label...")
            safe_click(previous_month_label)
            time.sleep(3)
        else:
            print("Calendar is already set to previous month.")
            
        return True
    except Exception as e:
        print(f"Error setting to previous month: {e}")
        try:
            previous_month_selector = "//input[@id='filterDate5']"
            previous_month_radio = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, previous_month_selector))
            )
            if not previous_month_radio.is_selected():
                print("Fallback: Clicking 'Previous month' radio button...")
                safe_click(previous_month_radio)
                time.sleep(3)
            return True
        except Exception as e2:
            print(f"Fallback also failed: {e2}")
            return False

def safe_click(element):
    """Robust click with JS fallback."""
    try:
        element.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception as e:
            print(f"safe_click failed: {e}")
            return False

def clear_all_filters():
    """
    Clear all existing filters by unchecking all checkboxes in the importance and currency sections.
    """
    try:
        print("Clearing all existing filters...")
        
        # Uncheck all importance checkboxes
        importance_container = driver.find_element(By.ID, "economicCalendarFilterImportance")
        importance_checkboxes = importance_container.find_elements(By.XPATH, ".//input[@type='checkbox']")
        
        for checkbox in importance_checkboxes:
            if checkbox.is_selected() or checkbox.get_attribute("checked"):
                safe_click(checkbox)
                time.sleep(0.2)
        
        # Uncheck all currency checkboxes (including "Select all")
        currency_container = driver.find_element(By.ID, "economicCalendarFilterCurrency")
        currency_checkboxes = currency_container.find_elements(By.XPATH, ".//input[@type='checkbox']")
        
        for checkbox in currency_checkboxes:
            if checkbox.is_selected() or checkbox.get_attribute("checked"):
                safe_click(checkbox)
                time.sleep(0.2)
        
        print("All filters cleared successfully")
        return True
        
    except Exception as e:
        print(f"Error clearing filters: {e}")
        return False

def apply_importance_filters(selected_importance):
    """
    Apply importance filters by checking the appropriate checkboxes.
    selected_importance: list of importance levels like ["High", "Medium", "Low", "Holidays"] or ["ALL"]
    """
    print("Applying importance filters...")
    
    importance_mapping = {
        "Holidays": "filterImportance1",
        "Low": "filterImportance2",
        "Medium": "filterImportance4",
        "High": "filterImportance8"
    }
    
    try:
        if "ALL" in [imp.upper() for imp in selected_importance]:
            print("  Selecting ALL importance levels")
            for importance, checkbox_id in importance_mapping.items():
                checkbox = driver.find_element(By.ID, checkbox_id)
                if not (checkbox.is_selected() or checkbox.get_attribute("checked")):
                    safe_click(checkbox)
                    print(f"    Selected: {importance}")
                time.sleep(0.2)
        else:
            for importance in selected_importance:
                if importance in importance_mapping:
                    checkbox_id = importance_mapping[importance]
                    checkbox = driver.find_element(By.ID, checkbox_id)
                    
                    if not (checkbox.is_selected() or checkbox.get_attribute("checked")):
                        safe_click(checkbox)
                        print(f"  Selected importance: {importance}")
                    else:
                        print(f"  Importance '{importance}' was already selected")
                else:
                    print(f"  Warning: Unknown importance level '{importance}'")
        
        print("Waiting 5 seconds for importance filters to apply...")
        time.sleep(5)
        return True
        
    except Exception as e:
        print(f"Error applying importance filters: {e}")
        return False

def apply_currency_filters(selected_currencies):
    """
    Apply currency filters by checking the appropriate checkboxes.
    selected_currencies: list of currency codes like ["USD", "EUR", "GBP"] or ["ALL"]
    """
    print("Applying currency filters...")
    
    currency_mapping = {
        "AUD": "filterCurrency32",
        "BRL": "filterCurrency1024",
        "CAD": "filterCurrency16",
        "CHF": "filterCurrency64",
        "CNY": "filterCurrency128",
        "EUR": "filterCurrency2",
        "GBP": "filterCurrency8",
        "HKD": "filterCurrency4096",
        "INR": "filterCurrency65536",
        "JPY": "filterCurrency4",
        "KRW": "filterCurrency2048",
        "MXN": "filterCurrency16384",
        "NOK": "filterCurrency131072",
        "NZD": "filterCurrency256",
        "SEK": "filterCurrency512",
        "SGD": "filterCurrency8192",
        "USD": "filterCurrency1",
        "ZAR": "filterCurrency32768"
    }
    
    try:
        if "ALL" in [curr.upper() for curr in selected_currencies]:
            print("  Selecting ALL currencies using 'Select all' checkbox")
            select_all_checkbox = driver.find_element(By.ID, "selectAllCurrencies")
            if not (select_all_checkbox.is_selected() or select_all_checkbox.get_attribute("checked")):
                safe_click(select_all_checkbox)
                print("    Selected: ALL currencies")
            else:
                print("    'Select all' was already selected")
        else:
            try:
                select_all_checkbox = driver.find_element(By.ID, "selectAllCurrencies")
                if select_all_checkbox.is_selected() or select_all_checkbox.get_attribute("checked"):
                    safe_click(select_all_checkbox)
                    print("  Unchecked 'Select all'")
                    time.sleep(1)
            except Exception as e:
                print(f"  Could not find or interact with 'Select all' checkbox: {e}")
            
            for currency in selected_currencies:
                if currency.upper() in currency_mapping:
                    checkbox_id = currency_mapping[currency.upper()]
                    try:
                        checkbox = driver.find_element(By.ID, checkbox_id)
                        
                        is_selected = checkbox.is_selected() or checkbox.get_attribute("checked")
                        if not is_selected:
                            safe_click(checkbox)
                            print(f"  Selected currency: {currency}")
                        else:
                            print(f"  Currency '{currency}' was already selected")
                    except Exception as e:
                        print(f"  Could not find checkbox for {currency}: {e}")
                else:
                    print(f"  Warning: Unknown currency code '{currency}'")
            
            if "ZAR" not in [curr.upper() for curr in selected_currencies]:
                zar_checkbox_id = currency_mapping["ZAR"]
                try:
                    zar_checkbox = driver.find_element(By.ID, zar_checkbox_id)
                    if zar_checkbox.is_selected() or zar_checkbox.get_attribute("checked"):
                        safe_click(zar_checkbox)
                        print("  Explicitly unchecked ZAR as it was not selected")
                except Exception as e:
                    print(f"  Could not find ZAR checkbox to uncheck: {e}")
        
        print("Waiting 5 seconds for currency filters to apply...")
        time.sleep(5)
        return True
        
    except Exception as e:
        print(f"Error applying currency filters: {e}")
        return False

def apply_filters(selected_currencies, selected_importance):
    """
    Main function to apply all filters with proper sequencing and delays.
    """
    print("Starting filter application process...")
    
    if not clear_all_filters():
        print("Warning: Could not clear all filters")
    
    if not apply_importance_filters(selected_importance):
        print("Warning: Could not apply importance filters")
    
    if not apply_currency_filters(selected_currencies):
        print("Warning: Could not apply currency filters")
    
    print("Filter application process completed")
    return True

def extract_year_from_week_range(week_range):
    if not week_range:
        return ""
    try:
        year_match = re.search(r'\d{4}$', week_range)
        if year_match:
            return year_match.group(0)
        if ',' in week_range:
            return week_range.split(',')[-1].strip()
        return ""
    except:
        return ""

def collect_data():
    global first_week_collected, last_week_collected
    
    week_range = get_current_week_range()
    current_year = extract_year_from_week_range(week_range)
    
    if week_range:
        if first_week_collected is None:
            first_week_collected = week_range
        last_week_collected = week_range

    calendar_body = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "div.ec-table__body#economicCalendarTable")
    ))

    data = []
    current_date = ""
    all_elements = calendar_body.find_elements(By.XPATH, "./div")

    for element in all_elements:
        try:
            classes = element.get_attribute("class") or ""

            if "ec-table__title" in classes:
                date_text = element.text.strip()
                if current_year and ',' in date_text:
                    current_date = f"{date_text.split(',')[0].strip()} {current_year}"
                else:
                    current_date = date_text

            elif "ec-table__nav" in classes:
                continue

            elif "ec-table__item" in classes:
                if not current_date:
                    current_date = "Unknown"

                row_data = {
                    "Date": current_date,
                    "Time": "All Day",
                    "Currency": "nan",
                    "Event": "nan",
                    "Impact": "nan",
                    "Actual": "nan",
                    "Forecast": "nan",
                    "Previous": "nan",
                    "WeekRange": week_range or "",
                    "IsHoliday": "False"
                }

                # Handle holiday events specifically
                if "ec-table__item_holiday" in classes:
                    try:
                        # For holiday events, the event name is in a different structure
                        event_elem = element.find_element(By.CSS_SELECTOR, "div.ec-table__col_event")
                        event_name = event_elem.text.strip()
                        row_data["Event"] = event_name if event_name else "Holiday"
                        
                        # Try to extract the specific date from the event if possible
                        if event_name and re.search(r'\d{1,2}\s+[A-Za-z]+\s+\d{4}', event_name):
                            date_match = re.search(r'(\d{1,2}\s+[A-Za-z]+\s+\d{4})', event_name)
                            row_data["Date"] = date_match.group(1)
                        
                        # Try to get currency for the holiday
                        try:
                            currency_elem = element.find_element(By.CSS_SELECTOR, "div.ec-table__col_currency div.ec-table__curency-name")
                            row_data["Currency"] = currency_elem.text.strip()
                        except:
                            # If no specific currency, mark as GLOBAL
                            row_data["Currency"] = "GLOBAL"

                        row_data["Impact"] = "holiday"
                        row_data["IsHoliday"] = "True"
                        row_data["Time"] = "All Day"
                        
                    except Exception as e:
                        print(f"Error parsing holiday event: {e}")
                        row_data["Event"] = "Holiday"
                        row_data["Currency"] = "GLOBAL"
                        row_data["Impact"] = "holiday"
                        row_data["IsHoliday"] = "True"

                    data.append(row_data)
                    continue

                elif "ec-table__item_meeting" in classes:
                    try:
                        event_elem = element.find_element(By.CSS_SELECTOR, "div.ec-table__col_event")
                        event_name = event_elem.text.strip()
                        row_data["Event"] = event_name if event_name else "Special Event"

                        try:
                            currency_elem = element.find_element(By.CSS_SELECTOR, "div.ec-table__col_currency div.ec-table__curency-name")
                            row_data["Currency"] = currency_elem.text.strip()
                        except:
                            row_data["Currency"] = "GLOBAL"

                        row_data["Impact"] = "special"
                        row_data["IsHoliday"] = "True"
                    except Exception as e:
                        print(f"Error parsing meeting/special event: {e}")
                        row_data["Event"] = "Special Event"
                        row_data["Currency"] = "UNKNOWN"
                        row_data["Impact"] = "special"
                        row_data["IsHoliday"] = "True"

                    data.append(row_data)
                    continue

                # Regular events
                try:
                    time_elem = element.find_element(By.CSS_SELECTOR, "div.ec-table__col_time div")
                    time_text = time_elem.text.strip()
                    row_data["Time"] = time_text if time_text else "All Day"
                except:
                    row_data["Time"] = "All Day"

                try:
                    currency_elem = element.find_element(By.CSS_SELECTOR, "div.ec-table__col_currency div.ec-table__curency-name")
                    row_data["Currency"] = currency_elem.text.strip()
                except:
                    pass

                try:
                    event_elem = element.find_element(By.CSS_SELECTOR, "div.ec-table__col_event a")
                    row_data["Event"] = event_elem.text.strip()
                except:
                    pass

                try:
                    impact_icon = element.find_element(By.CSS_SELECTOR, "span.ec-table__importance")
                    impact_class = impact_icon.get_attribute("class") or ""
                    if "high" in impact_class:
                        row_data["Impact"] = "high"
                    elif "medium" in impact_class:
                        row_data["Impact"] = "medium"
                    elif "low" in impact_class:
                        row_data["Impact"] = "low"
                    else:
                        row_data["Impact"] = "none"
                except:
                    pass

                try:
                    actual_elem = element.find_element(By.CSS_SELECTOR, "div.ec-table__col_actual span")
                    row_data["Actual"] = actual_elem.text.strip()
                except:
                    pass

                try:
                    forecast_elem = element.find_element(By.CSS_SELECTOR, "div.ec-table__col_forecast")
                    row_data["Forecast"] = forecast_elem.text.strip()
                except:
                    pass

                try:
                    previous_elem = element.find_element(By.CSS_SELECTOR, "div.ec-table__col_previous div")
                    row_data["Previous"] = previous_elem.text.strip()
                except:
                    pass

                data.append(row_data)

        except Exception as e:
            print(f"Error processing element: {str(e)}")
            continue

    return sorted(data, key=lambda x: (x["Date"], x["Time"]))

def save_events(events, filename):
    """
    Save events to CSV. Always appends to existing file, never overwrites.
    """
    existing_events = set()
    file_exists = os.path.exists(filename)

    # Read existing events to avoid duplicates
    if file_exists:
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    event_id = (row.get("Date", ""), row.get("Time", ""), row.get("Event", ""), row.get("Currency", ""))
                    existing_events.add(event_id)
        except Exception as e:
            print(f"Warning: Could not read existing file to check for duplicates: {e}")

    # Filter out events that already exist in the file
    new_events = []
    for event in events:
        event_id = (event["Date"], event["Time"], event["Event"], event["Currency"])
        if event_id not in existing_events:
            event_to_save = {
                "Date": event["Date"],
                "Time": event["Time"],
                "Currency": event["Currency"],
                "Event": event["Event"],
                "Impact": event["Impact"],
                "Actual": event["Actual"],
                "Forecast": event["Forecast"],
                "Previous": event["Previous"],
                "IsHoliday": event["IsHoliday"],
                "WeekRange": event.get("WeekRange", "")
            }
            new_events.append(event_to_save)

    if not new_events:
        return 0

    fieldnames = ["Date", "Time", "Currency", "Event", "Impact", "Actual", "Forecast", "Previous", "IsHoliday", "WeekRange"]

    # Always append to the file, never overwrite
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        # Only write header if file is new
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_events)

    return len(new_events)

def get_settings_hash(selected_currencies, selected_importance):
    """
    Create a unique hash for the current settings
    """
    currencies_str = "_".join(sorted([c.upper() for c in selected_currencies]))
    importance_str = "_".join(sorted([i.title() for i in selected_importance]))
    settings_str = f"{currencies_str}|{importance_str}"
    return hashlib.md5(settings_str.encode()).hexdigest()

def create_dynamic_filename(cfg, first_week, last_week, output_folder):
    """
    Create a dynamic filename based on settings and week range
    """
    # Get currency and importance settings
    currencies = "ALL" if "ALL" in [c.upper() for c in cfg["currencies"]] else "_".join(cfg["currencies"])
    importance = "ALL" if "ALL" in [i.upper() for i in cfg["importance"]] else "_".join(cfg["importance"])
    
    # Clean up week strings for filename
    def clean_week_string(week_str):
        if not week_str:
            return "UNKNOWN"
        cleaned = re.sub(r'[\\/*?:"<>|]', "", week_str.replace(" ", "_"))
        return cleaned[:50] if len(cleaned) > 50 else cleaned
    
    first_clean = clean_week_string(first_week)
    last_clean = clean_week_string(last_week)
    
    # Construct filename
    filename = f"MQL5_{currencies}_currencies_{importance}_impact_from_{first_clean}_to_{last_clean}.csv"
    
    # Ensure filename is not too long
    if len(filename) > 200:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"MQL5_{currencies}_{importance}_{timestamp}.csv"
    
    return os.path.join(output_folder, filename)

def initialize_driver(headless=True):
    """
    Initialize Chrome driver optimized for headless operation
    """
    chrome_options = Options()
    
    # Essential options for headless operation
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    
    # Window and rendering options
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    
    # Performance and resource optimization
    chrome_options.add_argument("--disable-images")
    chrome_options.add_argument("--blink-settings=imagesEnabled=false")
    chrome_options.add_argument("--disable-javascript")
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    
    # Security and network options
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--disable-features=TranslateUI")
    chrome_options.add_argument("--disable-ipc-flooding-protection")
    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--disable-default-apps")
    chrome_options.add_argument("--disable-component-extensions-with-background-pages")
    
    # Set user agent for headless mode
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Experimental options for better headless performance
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_experimental_option("prefs", {
        "profile.default_content_setting_values.notifications": 2,
        "profile.default_content_settings.popups": 0,
        "profile.managed_default_content_settings.images": 2,
    })
    
    try:
        print(f"Initializing Chrome driver in {'headless' if headless else 'GUI'} mode...")
        driver = webdriver.Chrome(options=chrome_options)
        driver.implicitly_wait(10)
        
        # Additional configurations for headless stability
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        return driver
    except Exception as e:
        print(f"Error initializing Chrome driver: {e}")
        # Fallback: try without some experimental options
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--window-size=1920,1080")
            driver = webdriver.Chrome(options=chrome_options)
            driver.implicitly_wait(10)
            print("Fallback driver initialization successful")
            return driver
        except Exception as e2:
            print(f"Fallback driver also failed: {e2}")
            raise

def parse_env_or_args():
    parser = argparse.ArgumentParser(description="MQL5 economic calendar scraper")
    parser.add_argument("--currencies", nargs="*", type=str, default=["ALL"],
                       help="Currency codes (e.g., USD EUR GBP) or 'ALL' for all currencies. Available: " + ", ".join(CURRENCY_LABELS.keys()) + ", ALL")
    parser.add_argument("--importance", nargs="*", type=str, default=["ALL"],
                       help="Importance levels (e.g., High Medium) or 'ALL' for all levels. Available: " + ", ".join(IMPORTANCE_LABELS.keys()) + ", ALL")
    parser.add_argument("--output", type=str, default=None, help="Output CSV path")
    parser.add_argument("--headless", action="store_true", default=True, help="Run Chrome in headless mode (default: True)")
    parser.add_argument("--start-url", type=str, default="https://www.mql5.com/en/economic-calendar", help="Calendar URL")
    parser.add_argument("--list-options", action="store_true", help="List all available currency and importance options")

    args = parser.parse_args()

    if args.list_options:
        print("\nAvailable Currency Codes:")
        for code, label in CURRENCY_LABELS.items():
            print(f"  {code}: {label}")
        print("  ALL: Select all currencies")
        print("\nAvailable Importance Levels:")
        for level in IMPORTANCE_LABELS.keys():
            print(f"  {level}")
        print("  ALL: Select all importance levels")
        print("\nExample usage:")
        print("  python scraper.py --currencies USD EUR GBP --importance High Medium")
        print("  python scraper.py --currencies ALL --importance High")
        print("  python scraper.py --currencies USD --importance ALL")
        print("  python scraper.py --currencies ALL --importance ALL")
        sys.exit(0)

    currencies = args.currencies
    importance = args.importance

    # Set defaults to ALL if no arguments provided
    if not currencies:
        currencies = ["ALL"]
        print("No currencies specified. Using default: ALL currencies")

    if not importance:
        importance = ["ALL"]
        print("No importance levels specified. Using default: ALL importance levels")

    valid_currencies = []
    if currencies:
        for curr in currencies:
            if curr.upper() == "ALL":
                valid_currencies = ["ALL"]
                break
            elif curr.upper() in CURRENCY_LABELS:
                valid_currencies.append(curr.upper())
            else:
                print(f"Warning: Unknown currency '{curr}'. Available: {', '.join(CURRENCY_LABELS.keys())}, ALL")

    valid_importance = []
    if importance:
        for imp in importance:
            if imp.upper() == "ALL":
                valid_importance = ["ALL"]
                break
            matched = None
            for key in IMPORTANCE_LABELS.keys():
                if imp.strip().lower() == key.lower():
                    matched = key
                    break
            if matched:
                valid_importance.append(matched)
            else:
                print(f"Warning: Unknown importance '{imp}'. Available: {', '.join(IMPORTANCE_LABELS.keys())}, ALL")

    if not valid_currencies:
        print("No valid currencies specified. Using default: ALL currencies")
        valid_currencies = ["ALL"]

    if not valid_importance:
        print("No valid importance levels specified. Using default: ALL importance levels")
        valid_importance = ["ALL"]

    return {
        "currencies": valid_currencies,
        "importance": valid_importance,
        "output": args.output,
        "headless": args.headless,
        "start_url": args.start_url
    }
def main():
    global driver, wait, shutdown_flag, graceful_exit, first_week_collected, last_week_collected, settings_hash

    cfg = parse_env_or_args()
    
    print(f"Configuration: currencies={cfg['currencies']}, importance={cfg['importance']}, headless={cfg['headless']}")

    # EXTRACTED FROM OLD CODE: Get the previous month name
    previous_month = get_previous_month_name()
    
    # Use Docker path when in Docker mode, otherwise use Windows path
    if os.environ.get('DOCKER_MODE') == 'true':
        main_folder_path = DEFAULT_OUTPUT_DIR
    else:
        main_folder_path = DEFAULT_OUTPUT_DIR
    
    # EXTRACTED FROM OLD CODE: Create the main folder path with the month-specific subfolder
    month_folder = f"{previous_month} Batch"
    output_folder = os.path.join(main_folder_path, month_folder)
    
    # EXTRACTED FROM OLD CODE: Create the folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    print(f"Using output folder: {output_folder}")

    # Create settings hash
    settings_hash = get_settings_hash(cfg["currencies"], cfg["importance"])
    print(f"Settings hash: {settings_hash}")

    # Signal handling for Linux
    if hasattr(signal, 'SIGINT'):
        signal.signal(signal.SIGINT, signal_handler)
    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Retry mechanism for driver initialization
        for attempt in range(3):
            try:
                driver = initialize_driver(headless=cfg["headless"])
                break
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    print("Retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    raise
        
        # Retry mechanism for page loading
        for attempt in range(3):
            try:
                print(f"Loading page: {cfg['start_url']}")
                driver.get(cfg["start_url"])
                wait = WebDriverWait(driver, 30)
                
                # Wait for the main calendar table to be present
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div#economicCalendarTable")))
                print("Page loaded successfully")
                break
            except Exception as e:
                print(f"Page load attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    print("Retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    print("Failed to load page after 3 attempts")
                    raise

        # Step 1: Click the "Current week" button first
        print("Clicking 'Current week' button...")
        if not set_to_current_week():
            print("Warning: Failed to set to current week, continuing anyway")

        # Step 2: Click the "Previous month" button
        print("Clicking 'Previous month' button...")
        if not set_to_previous_month():
            print("Warning: Failed to set to previous month, continuing anyway")

        # Apply filters automatically with selected currencies and importance
        print("Applying filters...")
        if not apply_filters(cfg["currencies"], cfg["importance"]):
            print("Warning: Failed to apply filters automatically. Continuing with default filters.")

        # Step 3: Collect data for the previous month
        print("Collecting data for the previous month...")
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.ec-table__item")))
            time.sleep(3)  # Wait for any dynamic content to load
        except:
            print("Timed out waiting for events - continuing anyway")

        week_data = collect_data()
        
        # Create output file
        output_file = create_dynamic_filename(cfg, first_week_collected, last_week_collected, output_folder)
        print(f"Created output file: {os.path.basename(output_file)}")
        
        events_saved = save_events(week_data, output_file)
        print(f"Collected {events_saved} events for the previous month")

        print("\nData collection for previous month completed successfully!")
        return 0

    except Exception as e:
        print(f"\nCritical error: {str(e)}")
        traceback.print_exc()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        screenshot_file = os.path.join(output_folder, f"error_{timestamp}.png")
        try:
            if driver:
                driver.save_screenshot(screenshot_file)
                print(f"Screenshot saved to {screenshot_file}")
        except:
            print("Failed to save screenshot")

        return 1

    finally:
        try:
            if driver:
                driver.quit()
                print("WebDriver closed")
        except:
            pass

if __name__ == "__main__":
    sys.exit(main())