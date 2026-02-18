import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
import re
from html import unescape

def download_bgg_csv_with_selenium(username, password, save_path="boardgames_ranks.zip"):
    """Download BGG CSV using Selenium with Chrome."""
    
    print("="*60)
    print("BGG Data Download - Automated with Selenium")
    print("="*60)
    
    # Login page with redirect parameter
    login_url = 'https://boardgamegeek.com/login?redirect_server=1'
    
    # Create Chrome WebDriver
    print("Starting Chrome browser...")
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Run without GUI
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    
    # driver = webdriver.Chrome(options=options)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    try:
        # Navigate to login page
        print("Navigating to login page...")
        driver.get(login_url)
        time.sleep(2)  # Give page time to load
        
        # Handle cookie consent popup
        try:
            print("Looking for consent button...")
            consent_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    "//button[@aria-label=\"I'm OK with that\"]"
                ))
            )
            consent_button.click()
            print("✓ Consent button clicked")
            time.sleep(1)
        except TimeoutException:
            print("No consent button found (or already dismissed)")
        except Exception as e:
            print(f"Consent button error (continuing anyway): {e}")
        
        # Try to find and click second consent button if it exists
        try:
            cookie_button = driver.find_element(By.XPATH, '//button[contains(text(), "I\'m OK with that")]')
            cookie_button.click()
            print("✓ 2nd consent button clicked")
            time.sleep(1)
        except NoSuchElementException:
            print("No 2nd consent button found")
        except Exception as e:
            print(f"2nd consent button error (continuing anyway): {e}")
        
        # Wait a bit for page to settle after consent clicks
        time.sleep(2)
        
        # Find login form elements
        print("Looking for login form...")
        try:
            username_input = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, 'inputUsername'))
            )
            print("✓ Found username field")
        except TimeoutException:
            print("❌ Could not find username field")
            driver.save_screenshot("login_form_error.png")
            with open("login_form_page.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            raise RuntimeError("Login form not found. Saved debug files.")
        
        password_input = driver.find_element(By.ID, 'inputPassword')
        print("✓ Found password field")
        
        signin_button = driver.find_element(By.XPATH, '//button[contains(text(), "Sign In")]')
        print("✓ Found sign-in button")
        
        # Enter credentials
        print("Entering credentials...")
        username_input.clear()
        username_input.send_keys(username)
        
        password_input.clear()
        password_input.send_keys(password)
        
        time.sleep(1)
        
        # Click sign in
        print("Clicking Sign In...")
        signin_button.click()
        
        # Wait for login to complete - look for user profile link
        print("Waiting for login to complete...")
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="/user/"]'))
            )
            print("✅ Login successful!")
        except TimeoutException:
            # Check if we're still on login page or if there's an error
            current_url = driver.current_url
            page_text = driver.page_source.lower()
            
            if "login" in current_url:
                driver.save_screenshot("login_failed.png")
                with open("login_failed_page.html", "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                
                if "invalid" in page_text or "incorrect" in page_text:
                    raise RuntimeError("Login failed - invalid credentials")
                else:
                    raise RuntimeError("Login failed - still on login page")
            else:
                # We may have redirected successfully, proceed
                print("⚠️  Couldn't verify login element, but URL changed - proceeding...")
        
        # Navigate to download page
        print("\nNavigating to download page...")
        driver.get('https://boardgamegeek.com/data_dumps/bg_ranks')
        time.sleep(3)
        
        # Get page source
        page_source = driver.page_source
        
        # Extract download URL
        print("Looking for download link...")
        pattern = r'<a\s+href="(https://geek-export-stats\.s3\.amazonaws\.com/boardgames_export/boardgames_ranks_[^"]+)"'
        match = re.search(pattern, page_source)
        
        if not match:
            with open("download_page_debug.html", "w", encoding="utf-8") as f:
                f.write(page_source)
            raise RuntimeError("Could not find download link. Saved page to download_page_debug.html")
        
        zip_url = unescape(match.group(1))
        print(f"✓ Found download URL")
        
        # Get cookies from Selenium session
        cookies = driver.get_cookies()
        print(f"✓ Extracted {len(cookies)} cookies")
        
        # Create requests session with Selenium cookies
        session = requests.Session()
        for cookie in cookies:
            session.cookies.set(cookie['name'], cookie['value'], domain=cookie.get('domain'))
        
        # Download using requests
        print("\nDownloading CSV file...")
        zip_resp = session.get(zip_url, stream=True)
        
        if zip_resp.status_code != 200:
            raise RuntimeError(f"Failed to download zip: {zip_resp.status_code}")
        
        # Save file with progress
        total_size = int(zip_resp.headers.get('content-length', 0))
        downloaded = 0
        
        with open(save_path, "wb") as f:
            for chunk in zip_resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size:
                        percent = (downloaded / total_size) * 100
                        print(f"\rProgress: {percent:.1f}%", end="", flush=True)
        
        print()
        file_size = os.path.getsize(save_path)
        print(f"✅ CSV downloaded successfully: {save_path} ({file_size:,} bytes)")
        return True
        
    except Exception as e:
        # Save debug info on error
        try:
            driver.save_screenshot("error_screenshot.png")
            with open("error_page_source.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            print("\n⚠️  Saved error_screenshot.png and error_page_source.html for debugging")
        except:
            pass
        raise e
        
    finally:
        driver.quit()

def main():
    username = os.getenv("BGG_USERNAME") or input("BGG Username: ")
    password = os.getenv("BGG_PASSWORD") or input("BGG Password: ")
    
    if not username or not password:
        print("❌ Error: Username and password required")
        exit(1)
    
    download_bgg_csv_with_selenium(username, password)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)