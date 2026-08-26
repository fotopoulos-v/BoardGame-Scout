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

_BROWSER_LOGIN_JS = """
const done = arguments[arguments.length - 1];
const username = arguments[0];
const password = arguments[1];
fetch('/login/api/v1', {
    method: 'POST',
    credentials: 'include',
    headers: {'Content-Type': 'application/json', 'Accept': 'application/json'},
    body: JSON.stringify({credentials: {username: username, password: password}})
}).then(function (r) {
    return r.text().then(function (t) { done({status: r.status, body: t.slice(0, 500)}); });
}).catch(function (e) { done({status: -1, body: String(e)}); });
"""

_CURRENT_USER_JS = """
const done = arguments[arguments.length - 1];
fetch('/api/users/current', {credentials: 'include', headers: {'Accept': 'application/json'}})
    .then(function (r) { return r.json(); })
    .then(function (j) { done(j); })
    .catch(function (e) { done({loggedIn: false, error: String(e)}); });
"""


def _browser_api_login(driver, username, password):
    """POST to BGG's JSON login API from inside the browser page.

    Running the request in-page means it inherits the Cloudflare clearance the
    browser already earned when it loaded the page. A plain `requests` call from
    a CI runner has no such clearance and BGG answers it with HTTP 403.
    """
    driver.set_script_timeout(60)
    return driver.execute_async_script(_BROWSER_LOGIN_JS, username, password)


def _current_user(driver):
    """Ask BGG who it thinks we are: {'loggedIn': bool, 'username': str|None, ...}."""
    driver.set_script_timeout(60)
    return driver.execute_async_script(_CURRENT_USER_JS)


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
    # options.add_argument('--headless')  # Run without GUI
    # options.add_argument('--no-sandbox')
    # options.add_argument('--disable-dev-shm-usage')
    # options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36')
    options.add_argument('--window-size=1920,1080')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    
    # driver = webdriver.Chrome(options=options)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")  # avoid cloudflare webdriver detection

        # Navigate to login page
        print("Navigating to login page...")
        driver.get(login_url)

        def _wait_for_page_ready(timeout=20):
            try:
                WebDriverWait(driver, timeout).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
            except TimeoutException:
                print(f"  ⚠️  Page readyState timeout after {timeout}s, continuing anyway")

        def _find_first(selectors, timeout=5, condition="presence"):
            for by, value in selectors:
                try:
                    if condition == "clickable":
                        return WebDriverWait(driver, timeout).until(
                            EC.element_to_be_clickable((by, value))
                        )
                    return WebDriverWait(driver, timeout).until(
                        EC.presence_of_element_located((by, value))
                    )
                except TimeoutException:
                    continue
            return None

        def _is_logged_in():
            """Authoritative session check - the header DOM is too easy to misread."""
            try:
                info = _current_user(driver)
            except Exception as e:
                print(f"  session check failed: {e}")
                return False
            if isinstance(info, dict) and info.get("loggedIn"):
                print(f"  session check: logged in as {info.get('username')!r}")
                return True
            return False

        USERNAME_SELECTORS = [
            (By.NAME, "username"),
            (By.ID, "inputUsername"),
            (By.CSS_SELECTOR, "input[type='text'][name*='user' i]"),
            (By.CSS_SELECTOR, "input[type='email']"),
            (By.CSS_SELECTOR, "input[autocomplete='username']"),
        ]

        PASSWORD_SELECTORS = [
            (By.NAME, "password"),
            (By.ID, "inputPassword"),
            (By.CSS_SELECTOR, "input[type='password']"),
            (By.CSS_SELECTOR, "input[autocomplete='current-password']"),
        ]

        SUBMIT_SELECTORS = [
            (By.CSS_SELECTOR, "button[type='submit']"),
            (By.CSS_SELECTOR, "input[type='submit']"),
            (By.XPATH, "//button[contains(., 'Sign In') or contains(., 'Log In') or contains(., 'Login')]"),
            (By.CSS_SELECTOR, "form button"),
        ]

        _wait_for_page_ready()
        time.sleep(1)
        
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
        _wait_for_page_ready()
        time.sleep(1)

        # Log in through BGG's JSON API from inside the browser. The page has
        # already cleared Cloudflare, so this request is accepted where the same
        # call made with `requests` from a CI runner is answered with HTTP 403.
        print("Logging in via BGG's JSON API (from inside the browser)...")
        api_logged_in = False
        try:
            result = _browser_api_login(driver, username, password) or {}
            status = result.get("status")
            body = result.get("body", "")
            print(f"  login API responded HTTP {status}: {body[:200]}")
            if status in (200, 204):
                api_logged_in = _is_logged_in()
                if not api_logged_in:
                    print("  ⚠️  API reported success but the session is still anonymous")
            elif status == 400 and "invalid username or password" in body.lower():
                raise RuntimeError(
                    "BGG rejected the credentials (HTTP 400 'Invalid username or password'). "
                    "Check the BGG_USERNAME / BGG_PASSWORD repository secrets."
                )
            else:
                print("  API login did not succeed — falling back to the login form")
        except RuntimeError:
            raise
        except Exception as e:
            print(f"  API login error: {e} — falling back to the login form")

        # If already authenticated, skip form handling.
        if api_logged_in:
            print("✓ Session authenticated via the API")
            username_input = None
            password_input = None
            signin_button = None
        else:
            # Find login form elements with fallback selectors.
            print("Looking for login form...")
            print(f"  Page title: {driver.title!r}  URL: {driver.current_url!r}")
            # Wait for ANY input to appear before trying specific selectors.
            try:
                WebDriverWait(driver, 25).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input"))
                )
                print("  ✓ At least one input field is present on the page")
            except TimeoutException:
                n_inputs = driver.execute_script("return document.querySelectorAll('input').length")
                print(f"  ⚠️  No inputs found after 25s wait — inputs_via_js={n_inputs}")

            username_input = _find_first(USERNAME_SELECTORS, timeout=20, condition="presence")
            password_input = _find_first(PASSWORD_SELECTORS, timeout=20, condition="presence")
            signin_button = _find_first(SUBMIT_SELECTORS, timeout=10, condition="clickable")

            # Last resort: JavaScript-based element discovery
            if not username_input:
                username_input = driver.execute_script(
                    "return document.querySelector("
                    "'input[name=\"username\"], input[type=\"text\"], input[type=\"email\"]')"
                )
                if username_input:
                    print("  ✓ Username field found via JS")
            if not password_input:
                password_input = driver.execute_script(
                    "return document.querySelector('input[type=\"password\"]')"
                )
                if password_input:
                    print("  ✓ Password field found via JS")

            if not username_input:
                print("❌ Could not find username field")
            else:
                print("✓ Found username field")
            if not password_input:
                print("❌ Could not find password field")
            else:
                print("✓ Found password field")

            if not username_input or not password_input:
                # Last chance: if login succeeded with a sticky cookie/session, proceed.
                if _is_logged_in():
                    print("⚠️  Login form not detected, but user appears already logged in")
                    username_input = None
                    password_input = None
                else:
                    driver.save_screenshot("login_form_error.png")
                    with open("login_form_page.html", "w", encoding="utf-8") as f:
                        f.write(driver.page_source)
                    raise RuntimeError("Login form not found. Saved debug files.")

            if signin_button:
                print("✓ Found sign-in button")
            else:
                print("⚠️  Could not find sign-in button, will submit via password field")
        
        # Enter credentials only if login form exists.
        if username_input and password_input:
            print("Entering credentials...")
            username_input.clear()
            username_input.send_keys(username)

            password_input.clear()
            password_input.send_keys(password)

            time.sleep(1)

            # Click sign in (or submit fallback).
            print("Submitting login...")
            if signin_button:
                signin_button.click()
            else:
                password_input.submit()
        
        # Confirm the session with BGG itself rather than scraping the header DOM.
        print("Verifying login...")
        logged_in = api_logged_in
        if not logged_in:
            deadline = time.time() + 30
            while True:
                if _is_logged_in():
                    logged_in = True
                    break
                if time.time() >= deadline:
                    break
                time.sleep(3)

        if logged_in:
            print("✅ Login successful!")
        else:
            driver.save_screenshot("login_failed.png")
            with open("login_failed_page.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            if "invalid username or password" in driver.page_source.lower():
                raise RuntimeError(
                    "Login failed - BGG rejected the credentials. "
                    "Check the BGG_USERNAME / BGG_PASSWORD repository secrets."
                )
            raise RuntimeError(
                f"Login failed - session is still anonymous (url: {driver.current_url})"
            )
        
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