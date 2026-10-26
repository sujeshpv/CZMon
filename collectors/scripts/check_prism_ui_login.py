"""Logs in to the Prism UI of every configured PC and PE.

Opens https://<ip>:9440/ in headless Chromium and submits the login form.
Prints JSON keyed by IP for the local_cli collector to store in the database.
Each value is {"login_attempt": true, "error": ""} on success, or
{"login_attempt": false, "Error": "<UI message>", "screen_record": "<base64 webm>"}
on failure. A successful login is signed out before the window closes.
The failure recording is kept; a successful attempt is not.
"""

import argparse
import base64
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

# Used only when an endpoint entry does not include credentials.
DEF_UNAME = "admin"
DEF_PWD = "Nutanix.123"

NAV_TIMEOUT_MS = 45000
FORM_TIMEOUT_MS = 25000
RESULT_TIMEOUT_MS = 30000
POLL_MS = 500

USERNAME_SELECTORS = [
  "[data-test='loginInputUsername']",
  "#inputUsername",
  "#userName",
  "input[name='username']",
  "input[name='user']",
  "input[name='j_username']",
  "input#username",
  "input[autocomplete='username']",
]
PASSWORD_SELECTORS = [
  "[data-test='loginInputPassword']",
  "#inputPassword",
  "input[name='password']",
  "input[name='j_password']",
  "input#password",
  "input[type='password']",
]
SUBMIT_SELECTORS = [
  "[data-test='loginButtonSubmit']",
  ".n-login-btn",
  "button[type='submit']",
  "button:has-text('Log In')",
  "button:has-text('Login')",
]
WELCOME_ACCEPT_SELECTORS = [
  "#acceptWelcomeBanner",
  "#btnAcceptWelcomeBanner",
  "[data-test='welcomeBannerAcceptBtn']",
  "[data-test='welcomeBannerAcceptButton']",
  "[data-test='welcome-banner-accept']",
  ".welcome-banner-accept",
  ".n-welcome-banner button",
  "#popupWelcomeBanner button",
  "#welcome-banner-modal button",
  "#welcomeBannerModal button",
]
WELCOME_ROOT_SELECTORS = [
  "#popupWelcomeBanner",
  "#welcome-banner-modal",
  "#welcomeBannerModal",
  ".n-welcome-banner",
  ".welcome-banner",
  "[data-test='welcomeBanner']",
  "[data-test='welcome-banner']",
]
LOCAL_ACCOUNT_SELECTORS = [
  "[data-test='loginLinkLocal']",
  "[data-test='localLoginLink']",
  "[data-test='providersLinkLocal']",
  "a:has-text('Nutanix local account')",
  "button:has-text('Nutanix local account')",
  "[role='link']:has-text('Nutanix local account')",
  "[role='button']:has-text('Nutanix local account')",
]
PROVIDER_SELECTORS = [
  "[data-test='loginInputProvider']",
  "[data-test='identityProviderSelect']",
  "select#inputProvider",
  "select[name='provider']",
  "select[name='directory']",
]
ERROR_SELECTORS = [
  "[data-test='alert']",
  "[data-test='loginError']",
  "[data-test='alert-message']",
  ".error-message",
  ".error-messages",
  ".login-error",
  ".alert-error",
  ".alert-danger",
  ".n-error-message",
  ".n-notification-error",
  "[role='alert']",
]
USER_MENU_SELECTORS = [
  "[data-test='header-user-btn']",
  "[data-test='user-menu-button']",
  "[data-test='btn_user_menu']",
  "[data-test='btn_user']",
  "[data-test='user-menu']",
  "[data-test='userdropdown']",
  "[data-test='account-menu']",
  "[data-test='nc-user-menu']",
  "[data-test='user-dropdown']",
  "[data-test='user_menu']",
  "[aria-label*='user menu' i]",
  "[aria-label*='account' i]",
  "button[aria-label*='user' i]",
  ".n-username",
  ".n-user-menu",
  "button.user-name",
]
SIGNOUT_SELECTORS = [
  "[data-test='sign-out']",
  "[data-test='signout']",
  "[data-test='signoutlink']",
  "[data-test='user-menu-item-sign-out']",
  "[data-test='user-signout']",
  "[data-test='logout']",
  "[data-test='user-menu-sign-out']",
  ".n-logout",
]
VIDEO_WIDTH = 640
VIDEO_HEIGHT = 360
# Login counts only after the Prism shell is up: the menu button or Settings.
SHELL_SELECTORS = [
  "[data-test='header-hamburger-button']",
  "[data-test='hamburger-button']",
  "[data-test='nav-hamburger']",
  "[data-test='menu-hamburger']",
  "[data-test='explore-nav']",
  "[aria-label*='hamburger' i]",
  "[aria-label*='main menu' i]",
  "[aria-label*='navigation menu' i]",
  "[aria-label*='open menu' i]",
  "[data-test='header-settings-btn']",
  "[data-test='settings-btn']",
  "[data-test='btn_settings']",
  "[data-test='settings-icon']",
  "[data-test='settingsmenu']",
  "[data-test*='settings' i]",
  "[aria-label='Settings' i]",
  "[aria-label*='settings' i]",
  "[title='Settings' i]",
  ".n-settings-btn",
  ".n-settings",
  ".n-hamburger",
  ".n-nav-toggle",
  "button.n-nav-toggle",
]
AUTH_FAIL_HINTS = (
  "invalid",
  "incorrect",
  "does not match",
  "do not match",
  "did not match",
  "denied",
  "locked",
  "unauthorized",
  "authentication failed",
  "bad credentials",
  "try again",
)

logger = logging.getLogger(__name__)

# Top-bar Settings gear or hamburger. Login is not successful until one shows.
SHELL_JS = """
() => {
  const nodes = document.querySelectorAll(
    'button, a, [role="button"], [title], [aria-label], [data-test]'
  );
  for (const el of nodes) {
    const rect = el.getBoundingClientRect();
    if (rect.top > 160 || rect.width < 8 || rect.height < 8 || rect.bottom < 0) {
      continue;
    }
    const style = getComputedStyle(el);
    if (style.display === 'none' || style.visibility === 'hidden') continue;
    if (Number(style.opacity) === 0) continue;
    const aria = (el.getAttribute('aria-label') || '').toLowerCase();
    const title = (el.getAttribute('title') || '').toLowerCase();
    const test = (el.getAttribute('data-test') || '').toLowerCase();
    const text = (el.innerText || '').replace(/\\s+/g, ' ').trim().toLowerCase();
    if (text.length > 24) continue;
    if (aria.includes('setting') || title === 'settings' || text === 'settings') {
      return true;
    }
    if (
      aria.includes('hamburger') || aria.includes('main menu')
      || aria.includes('navigation menu') || aria.includes('open menu')
    ) {
      return true;
    }
    if (
      test.includes('hamburger') || test.includes('settings')
      || test.includes('nav-toggle')
    ) {
      return true;
    }
    const cls = typeof el.className === 'string' ? el.className.toLowerCase() : '';
    if (
      cls.includes('n-settings') || cls.includes('hamburger')
      || cls.includes('nav-toggle')
    ) {
      return true;
    }
  }
  return false;
}
"""

# Marks one Skip / Close / Acknowledge control inside a license or tour popup.
DISMISS_POPUP_JS = """
() => {
  const bad = /\\b(next|learn more|update|buy|get started|take a tour|start tour|continue)\\b/i;
  const good = /^(skip(\\s+tour)?|close(\\s+tour)?|not now|no,?\\s*thanks|maybe later|remind me later|ask me later|got it!?|acknowledge|dismiss|done|end tour|exit|ok|okay|×|x)$/i;
  function shown(el) {
    if (!el || !el.getBoundingClientRect) return false;
    const style = getComputedStyle(el);
    if (style.display === 'none' || style.visibility === 'hidden') return false;
    if (Number(style.opacity) === 0) return false;
    const rect = el.getBoundingClientRect();
    return rect.width > 8 && rect.height > 8 && rect.bottom > 0 && rect.right > 0;
  }
  function rootFrom(start) {
    let el = start;
    for (let i = 0; i < 10 && el && el !== document.body; i += 1) {
      const role = el.getAttribute ? (el.getAttribute('role') || '') : '';
      const test = el.getAttribute ? (el.getAttribute('data-test') || '') : '';
      const cls = typeof el.className === 'string' ? el.className : '';
      const blob = (cls + ' ' + test + ' ' + role).toLowerCase();
      const text = ((el.innerText || '') + '').slice(0, 500);
      const lower = text.toLowerCase();
      if (/sign out|log out/.test(lower) && !/license|tour/.test(lower)) {
        el = el.parentElement;
        continue;
      }
      if (role === 'dialog' || role === 'alertdialog') return el;
      if (/modal|popup|tour|license|onboarding|joyride|pendo|overlay/.test(blob)) {
        return el;
      }
      const style = getComputedStyle(el);
      const z = parseInt(style.zIndex, 10) || 0;
      if ((style.position === 'fixed' || style.position === 'absolute') && z >= 50
          && /license|tour|what's new|whats new|expir|onboarding|new feature/.test(lower)) {
        return el;
      }
      el = el.parentElement;
    }
    return null;
  }
  const roots = [];
  const seen = new Set();
  function add(el) {
    const root = rootFrom(el);
    if (root && shown(root) && !seen.has(root)) {
      seen.add(root);
      roots.push(root);
    }
  }
  document.querySelectorAll(
    '[role="dialog"], [role="alertdialog"], [class*="modal" i], [class*="Modal"],'
    + '[class*="popup" i], [class*="tour" i], [class*="onboarding" i],'
    + '[class*="joyride"], [class*="pendo"], [class*="licens" i],'
    + '[data-test*="license" i], [data-test*="tour" i], [data-test*="modal" i],'
    + '.n-licensing-popup, #popupLicensing'
  ).forEach(add);
  const cx = Math.floor(window.innerWidth / 2);
  const cy = Math.floor(window.innerHeight / 2);
  (document.elementsFromPoint(cx, cy) || []).forEach(add);
  let best = null;
  let bestScore = -1;
  let bestLabel = '';
  roots.forEach((root) => {
    root.querySelectorAll(
      'button, a, [role="button"], [aria-label], [data-test*="close" i], [data-test*="skip" i]'
    ).forEach((btn) => {
      if (!shown(btn)) return;
      const aria = btn.getAttribute('aria-label') || btn.getAttribute('title') || '';
      const test = btn.getAttribute('data-test') || '';
      const text = ((btn.innerText || '') + '').replace(/\\s+/g, ' ').trim();
      const blob = (text + ' ' + aria + ' ' + test).trim();
      if (!blob || bad.test(blob)) return;
      const ok = good.test(text) || good.test(aria) || /close|skip|ack/i.test(test)
        || /^close/i.test(aria);
      if (!ok) return;
      const lower = blob.toLowerCase();
      let score = 1;
      if (/skip/.test(lower)) score = 5;
      else if (/acknowledge|remind me later|not now|got it|maybe later|no thanks/.test(lower)) {
        score = 4;
      } else if (/close/.test(lower)) score = 3;
      if (score <= bestScore) return;
      best = btn;
      bestScore = score;
      bestLabel = text || aria || 'Close';
    });
  });
  document.querySelectorAll('[data-czmon-dismiss]').forEach((node) => {
    node.removeAttribute('data-czmon-dismiss');
  });
  if (!best) return null;
  best.setAttribute('data-czmon-dismiss', '1');
  return {label: bestLabel.slice(0, 60)};
}
"""
_BASE_DIR = os.path.dirname(
  os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)


def _selector(selectors):
  """Join Playwright selectors into one OR locator."""
  return ", ".join(selectors)


def _first_visible(page, selectors, timeout_ms):
  """Return the first matching locator once it is visible.

  Args:
    page: Playwright page or frame.
    selectors (list): Selector strings tried as one OR locator.
    timeout_ms (int): How long to wait for a visible match.

  Returns:
    Locator: The first visible match.
  """
  locator = page.locator(_selector(selectors)).first
  locator.wait_for(state="visible", timeout=timeout_ms)
  return locator


def _visible_text(page, selectors):
  """Return text from the first visible match, or an empty string.

  Args:
    page: Playwright page.
    selectors (list): Selector strings tried as one OR locator.

  Returns:
    str: Visible text, or "" when nothing matches.
  """
  locator = page.locator(_selector(selectors))
  count = min(locator.count(), 5)
  for index in range(count):
    item = locator.nth(index)
    try:
      if not item.is_visible():
        continue
      text = " ".join(item.inner_text().split())
    except Exception:
      continue
    if text:
      return text
  return ""


def _password_visible(page):
  """Return True when a password field is still on the login form.

  Args:
    page: Playwright page.

  Returns:
    bool: True if a password input is visible in the page or a child frame.
  """
  try:
    frames = _frames(page)
  except Exception:
    frames = [page]
  for frame in frames:
    locator = frame.locator(_selector(PASSWORD_SELECTORS)).first
    try:
      if locator.is_visible():
        return True
    except Exception:
      continue
  return False


def _shell_visible(page):
  """Return True when the Prism menu button or Settings control is showing.

  Args:
    page: Playwright page after the login form was submitted.

  Returns:
    bool: True when the signed-in shell is on screen.
  """
  for frame in _frames(page):
    if _first_visible_now(frame, SHELL_SELECTORS):
      return True
    named = _visible_named(frame.get_by_role(
      "button",
      name=re.compile(r"settings|hamburger|main menu", re.I),
    ))
    if named is not None:
      return True
    try:
      if frame.evaluate(SHELL_JS):
        return True
    except Exception:
      continue
  return False


def _logged_in(page):
  """Return True only after Settings or the menu button is visible.

  Args:
    page: Playwright page after the login button was clicked.

  Returns:
    bool: True when the Prism shell has rendered.
  """
  return _shell_visible(page)


def _is_auth_error(text):
  """Return True when visible text is a rejected-login message.

  Args:
    text (str): Alert text from the login page.

  Returns:
    bool: True if the text describes a failed authentication.
  """
  lowered = (text or "").lower()
  return any(hint in lowered for hint in AUTH_FAIL_HINTS)


def _credentials(endpoint):
  """Resolve username and password for one endpoint entry.

  Args:
    endpoint (dict): One PC or PE record from endpoints.json.

  Returns:
    tuple: (username, password). Missing values fall back to the defaults.
  """
  nested = endpoint.get("credentials") or {}
  user = (
    endpoint.get("user")
    or endpoint.get("username")
    or nested.get("username")
    or nested.get("user")
    or DEF_UNAME
  )
  password = endpoint.get("password") or nested.get("password") or DEF_PWD
  return user, password


def _load_json(path):
  """Load a JSON file.

  Args:
    path (str): Absolute or relative path.

  Returns:
    dict: Parsed JSON object.
  """
  with open(path, "r", encoding="utf-8") as handle:
    return json.load(handle)


def _rows_from_config(config_data):
  """Normalize both endpoints.json shapes into PC and PE rows.

  Args:
    config_data (dict): Parsed endpoints document.

  Returns:
    list: Endpoint dicts with a ``type`` of PC or PE.
  """
  rows = []
  if not isinstance(config_data, dict):
    return rows
  if "pcs" in config_data or "pes" in config_data:
    for item in config_data.get("pcs") or []:
      if isinstance(item, dict):
        row = dict(item)
        row["type"] = "PC"
        rows.append(row)
    for item in config_data.get("pes") or []:
      if isinstance(item, dict):
        row = dict(item)
        row["type"] = str(row.get("type") or "PE").upper()
        rows.append(row)
    return rows
  for zone, entries in config_data.items():
    if not isinstance(entries, list):
      continue
    for item in entries:
      if not isinstance(item, dict):
        continue
      row = dict(item)
      row["zone"] = zone
      row["type"] = str(row.get("type") or "").upper()
      rows.append(row)
  return rows


def _usable_rows(path):
  """Return endpoint rows that have an IP address.

  Args:
    path (str): Path to an endpoints JSON file.

  Returns:
    list: Rows whose ``ip`` or ``virtual_ip`` is non-empty.
  """
  try:
    rows = _rows_from_config(_load_json(path))
  except Exception:
    return []
  usable = []
  for row in rows:
    ip = str(row.get("ip") or row.get("virtual_ip") or "").strip()
    if ip:
      row["ip"] = ip
      usable.append(row)
  return usable


def resolve_config_path(explicit=None):
  """Pick the endpoints file that lists PC and PE addresses.

  ``static/configurations/endpoints.json`` is preferred when it contains
  IPs. Otherwise the script uses ``configurations/endpoints.json``.

  Args:
    explicit (str, optional): Caller-supplied config path.

  Returns:
    str: Path of the endpoints file to read.
  """
  if explicit:
    return explicit
  static_path = os.path.join(
    _BASE_DIR, "static", "configurations", "endpoints.json"
  )
  listed_path = os.path.join(_BASE_DIR, "configurations", "endpoints.json")
  if _usable_rows(static_path):
    return static_path
  if _usable_rows(listed_path):
    return listed_path
  return static_path


def _shortest_auth_text(locator):
  """Return the shortest visible auth-error string from a locator.

  Args:
    locator: Playwright locator that may match the alert and its parents.

  Returns:
    str: The UI error text, or "" when none of the matches is an auth error.
  """
  best = ""
  try:
    count = min(locator.count(), 8)
  except Exception:
    return ""
  for index in range(count):
    item = locator.nth(index)
    try:
      if not item.is_visible():
        continue
      text = " ".join(item.inner_text().split())
    except Exception:
      continue
    if not text or len(text) > 400 or not _is_auth_error(text):
      continue
    if not best or len(text) < len(best):
      best = text
  return best


def _ui_error_text(page):
  """Read the login error currently shown by Prism.

  Args:
    page: Playwright page after the login form was submitted.

  Returns:
    str: The visible alert text, or "" when Prism has not shown one.
  """
  from_alert = _shortest_auth_text(page.locator(_selector(ERROR_SELECTORS)))
  if from_alert:
    return from_alert
  hinted = page.get_by_text(
    re.compile(
      r"incorrect|invalid|does not match|did not match|do not match|try again",
      re.I,
    )
  )
  return _shortest_auth_text(hinted)


def wait_for_login_result(page):
  """Poll until Prism accepts the session or shows a login error.

  Args:
    page: Playwright page after the login form was submitted.

  Returns:
    tuple: (login_ok, error_message). error_message is the text Prism showed.
  """
  deadline = time.monotonic() + (RESULT_TIMEOUT_MS / 1000)
  while time.monotonic() < deadline:
    error_text = _ui_error_text(page)
    if error_text:
      return False, error_text
    if _logged_in(page):
      logger.info("Settings or menu button is visible")
      return True, ""
    if not _password_visible(page):
      _close_popup_once(page)
    page.wait_for_timeout(POLL_MS)
  if _logged_in(page):
    logger.info("Settings or menu button is visible")
    return True, ""
  error_text = _ui_error_text(page)
  if error_text:
    return False, error_text
  return False, "Timed out waiting for the Prism UI after login"


def _frames(page):
  """Return the page plus every child frame.

  Args:
    page: Playwright page.

  Returns:
    list: Frames to search. The main page is first.
  """
  frames = []
  try:
    frames.extend(page.frames)
  except Exception:
    frames = []
  if page.main_frame not in frames:
    frames.insert(0, page.main_frame)
  return frames or [page.main_frame]


def _find_user_menu(page, username):
  """Find the header control that opens the Prism user menu.

  PE uses ``.n-username``. Newer Prism Central uses a header button that
  those PE selectors do not match, often labeled with the username.

  Args:
    page: Playwright page on the signed-in UI.
    username (str): Account name that was used to log in.

  Returns:
    Locator or None: The visible user-menu control.
  """
  name_re = re.compile(r"^%s$" % re.escape(username or ""), re.I)
  for frame in _frames(page):
    marked = _mark_user_menu(frame, username)
    if marked is not None:
      return marked
    found = _first_visible_now(frame, USER_MENU_SELECTORS)
    if found is not None:
      return found
    if not username:
      continue
    for role in ("button", "link"):
      named = _visible_named(frame.get_by_role(role, name=name_re))
      if named is not None:
        return named
    named = _visible_named(frame.get_by_text(name_re))
    if named is not None:
      return named
  return None


def _mark_user_menu(frame, username):
  """Mark the header account control, including one that is not a button.

  Args:
    frame: Playwright frame to search.
    username (str): Account name shown in the Prism header.

  Returns:
    Locator or None: The marked account control.
  """
  try:
    label = frame.evaluate(
      """(username) => {
        const want = (username || '').trim().toLowerCase();
        const nodes = document.querySelectorAll(
          'button, a, [role="button"], .n-username, [class*="username" i], [data-test*="user" i]'
        );
        let best = null;
        nodes.forEach((el) => {
          const rect = el.getBoundingClientRect();
          if (rect.width < 8 || rect.height < 8 || rect.top > 160 || rect.bottom < 0) {
            return;
          }
          const style = getComputedStyle(el);
          if (style.display === 'none' || style.visibility === 'hidden') return;
          const text = ((el.innerText || '') + '').replace(/\\s+/g, ' ').trim().toLowerCase();
          const aria = (el.getAttribute('aria-label') || '').trim().toLowerCase();
          const test = (el.getAttribute('data-test') || '').toLowerCase();
          const cls = typeof el.className === 'string' ? el.className.toLowerCase() : '';
          const isUser = (want && (text === want || aria === want))
            || cls.includes('n-username')
            || test.includes('header-user')
            || test.includes('user-menu')
            || test.includes('user_menu');
          if (!isUser) return;
          if (text === 'skip to main content') return;
          best = el;
        });
        document.querySelectorAll('[data-czmon-user]').forEach((node) => {
          node.removeAttribute('data-czmon-user');
        });
        if (!best) return null;
        best.setAttribute('data-czmon-user', '1');
        return ((best.innerText || best.getAttribute('aria-label') || 'user') + '')
          .replace(/\\s+/g, ' ').trim().slice(0, 40);
      }""",
      username or "",
    )
  except Exception:
    return None
  if not label:
    return None
  button = frame.locator("[data-czmon-user='1']").first
  try:
    if button.is_visible():
      return button
  except Exception:
    return None
  return None


def _find_sign_out(page):
  """Find a visible Sign Out or Log Out control.

  Args:
    page: Playwright page, after the user menu was opened when needed.

  Returns:
    Locator or None: The sign-out control.
  """
  label = re.compile(r"sign out|log out|logout", re.I)
  exact = re.compile(r"^\s*(sign out|log out|logout)\s*$", re.I)
  for frame in _frames(page):
    for role in ("menuitem", "button", "link"):
      found = _visible_named(frame.get_by_role(role, name=label))
      if found is not None:
        return found
    found = _visible_named(frame.get_by_text(exact))
    if found is not None:
      return found
    found = _first_visible_now(frame, SIGNOUT_SELECTORS)
    if found is not None:
      return found
  return None


OVERLAY_SELECTORS = [
  ".modal.show",
  ".modal.in",
  ".ntnx-modal",
  "[class*='ntnx-modal']",
  ".n-licensing-popup",
  "#popupLicensing",
  ".introjs-tooltip",
  "[class*='whats-new']",
  "[class*='whatsNew']",
  "[class*='product-tour']",
  "[class*='joyride']",
  "[class*='pendo']",
  "[data-test*='whats-new']",
  "[data-test*='whatsnew']",
  "[data-test='license-popup']",
  "[data-test*='license-expiry']",
  "[data-test*='licenseExpiry']",
]
OVERLAY_CLOSE_SELECTORS = [
  "[aria-label='Close' i]",
  "[data-test='close']",
  "[data-test='close-button']",
  "[data-test='modal-close-btn']",
  "[data-dismiss='modal']",
  "button.close",
  ".introjs-skipbutton",
]
# These labels only appear on the license reminder and the product tour.
POPUP_BUTTON_RE = re.compile(
  r"^\s*(skip(\s+tour)?|not now|no,?\s*thanks|maybe later|"
  r"remind me later|ask me later|got it!?|acknowledge|end tour)\s*$",
  re.I,
)
# Close / Cancel / OK are common in the app, so they are used only inside a dialog.
POPUP_DIALOG_BUTTON_RE = re.compile(
  r"^\s*(skip(\s+tour)?|close|not now|no,?\s*thanks|maybe later|"
  r"remind me later|got it|dismiss|cancel|ok|okay|acknowledge)\s*$",
  re.I,
)


def _last_visible(locator):
  """Return the last visible match, which is usually the topmost popup.

  Args:
    locator: Playwright locator.

  Returns:
    Locator or None: The last visible match.
  """
  try:
    count = min(locator.count(), 8)
  except Exception:
    return None
  found = None
  for index in range(count):
    item = locator.nth(index)
    try:
      if item.is_visible():
        found = item
    except Exception:
      continue
  return found


def _overlay_roots(page):
  """Return visible dialogs that can cover the Prism header.

  Args:
    page: Playwright page.

  Returns:
    list: Visible dialog or modal locators.
  """
  roots = []
  for frame in _frames(page):
    for role in ("dialog", "alertdialog"):
      locator = frame.get_by_role(role)
      try:
        count = min(locator.count(), 12)
      except Exception:
        continue
      for index in range(count):
        item = locator.nth(index)
        try:
          if item.is_visible():
            roots.append(item)
        except Exception:
          continue
    locator = frame.locator(_selector(OVERLAY_SELECTORS))
    try:
      count = min(locator.count(), 12)
    except Exception:
      continue
    for index in range(count):
      item = locator.nth(index)
      try:
        if item.is_visible():
          roots.append(item)
      except Exception:
        continue
  return roots


def _click_popup_button(root, name_re):
  """Click a dismiss control inside one popup.

  Args:
    root: Dialog locator, or a frame when the popup has no dialog role.
    name_re: Compiled pattern for the button label.

  Returns:
    str: Label that was clicked, or "" when nothing was clicked.
  """
  for role in ("button", "link"):
    button = _last_visible(root.get_by_role(role, name=name_re))
    if button is None:
      continue
    try:
      label = " ".join(button.inner_text().split()) or role
      button.click()
      return label
    except Exception:
      continue
  button = _last_visible(root.locator(_selector(OVERLAY_CLOSE_SELECTORS)))
  if button is None:
    return ""
  try:
    button.click()
  except Exception:
    return ""
  return "Close"


def _js_dismiss_popup(page):
  """Click the dismiss control inside a license or tour popup.

  Prism Central draws these dialogs without the classes the older selector
  list matches. The page script finds the top dialog and marks one button.

  Args:
    page: Playwright page.

  Returns:
    bool: True when a popup button was clicked.
  """
  for frame in _frames(page):
    try:
      found = frame.evaluate(DISMISS_POPUP_JS)
    except Exception:
      continue
    if not found:
      continue
    button = frame.locator("[data-czmon-dismiss='1']").first
    label = (found.get("label") or "Close")[:60]
    try:
      button.click(timeout=4000)
    except Exception:
      try:
        button.click(timeout=4000, force=True)
      except Exception:
        continue
    logger.info("Closing popup (%s)", label)
    return True
  return False


def _close_popup_once(page):
  """Close one license reminder or product-tour popup, if one is showing.

  The click is Skip, Close, or Acknowledge. That only hides the dialog for
  this browser session. It does not change licensing or start the tour.

  Args:
    page: Playwright page.

  Returns:
    bool: True when a popup was closed.
  """
  if _js_dismiss_popup(page):
    return True
  for root in _overlay_roots(page):
    label = _click_popup_button(root, POPUP_DIALOG_BUTTON_RE)
    if not label:
      continue
    logger.info("Closing popup (%s)", label[:60])
    return True
  for frame in _frames(page):
    label = ""
    for role in ("button", "link"):
      button = _last_visible(frame.get_by_role(role, name=POPUP_BUTTON_RE))
      if button is None:
        continue
      try:
        label = " ".join(button.inner_text().split()) or role
        button.click()
      except Exception:
        label = ""
        continue
      break
    if not label:
      continue
    logger.info("Closing popup (%s)", label[:60])
    return True
  return False


def _header_control_names(page):
  """List the visible controls in the top bar.

  Args:
    page: Playwright page.

  Returns:
    list: Short labels for the header buttons, for the logout warning.
  """
  try:
    names = page.evaluate(
      """() => {
        const rows = [];
        document.querySelectorAll('button, a, [role="button"]').forEach((el) => {
          const rect = el.getBoundingClientRect();
          if (rect.top > 120 || rect.width < 8 || rect.height < 8) return;
          const style = getComputedStyle(el);
          if (style.display === 'none' || style.visibility === 'hidden') return;
          const text = (
            el.innerText || el.getAttribute('aria-label')
            || el.getAttribute('data-test') || ''
          ).replace(/\\s+/g, ' ').trim().slice(0, 40);
          if (text) rows.push(text);
        });
        return rows.slice(0, 12);
      }"""
    )
  except Exception:
    return []
  return names or []


def _find_sign_out_confirm(page):
  """Find a Sign Out button inside a confirmation dialog.

  Args:
    page: Playwright page after the user-menu Sign Out was clicked.

  Returns:
    Locator or None: The confirm button.
  """
  label = re.compile(r"^\s*(sign out|log out|logout|yes)\s*$", re.I)
  selector = (
    "[role='dialog'], [role='alertdialog'], [class*='ntnx-modal'], "
    ".modal.show, .modal.in"
  )
  for frame in _frames(page):
    roots = frame.locator(selector)
    try:
      count = min(roots.count(), 6)
    except Exception:
      continue
    for index in range(count):
      root = roots.nth(index)
      try:
        if not root.is_visible():
          continue
      except Exception:
        continue
      found = _visible_named(root.get_by_role("button", name=label))
      if found is not None:
        return found
  return None


def _back_at_login(page):
  """Return True when Prism is showing the login screen again.

  Prism Central often returns to a login screen whose address does not
  contain ``login``, and it shows the username box before the password box.
  A Settings control can still be in the page behind that form, so the
  login form is checked first.

  Args:
    page: Playwright page after Sign Out was clicked.

  Returns:
    bool: True when the login screen is visible.
  """
  if _password_visible(page):
    return True
  url = (page.url or "").lower()
  if any(part in url for part in ("login", "signin", "sign-in", "/iam")):
    return True
  login_text = re.compile(
    r"log\s*in|nutanix local account|welcome to prism",
    re.I,
  )
  for frame in _frames(page):
    if _first_visible_now(frame, USERNAME_SELECTORS):
      return True
    if _visible_named(frame.get_by_role("button", name=login_text)):
      return True
    if _visible_named(frame.get_by_text(login_text)):
      return True
  return False


def _watch_and_close_popups(page, seconds):
  """Wait for late popups and close each one before Sign Out.

  Prism Central often opens the license reminder and the product tour two
  or three seconds after the dashboard appears. Signing out in that gap
  fails when the next dialog covers the user menu.

  Args:
    page: Playwright page on the signed-in UI.
    seconds (float): How long to watch when no popup is showing yet.

  Returns:
    None
  """
  deadline = time.monotonic() + seconds
  closed = 0
  quiet = 0
  while time.monotonic() < deadline and closed < 4:
    if _close_popup_once(page):
      closed += 1
      quiet = 0
      deadline = max(deadline, time.monotonic() + 3)
      page.wait_for_timeout(400)
      continue
    quiet += 1
    if closed and quiet >= 3:
      return
    if not closed and quiet >= 6:
      return
    page.wait_for_timeout(POLL_MS)


def _logout(page, username):
  """Sign out of Prism after a successful login.

  Args:
    page: Playwright page on the signed-in Prism UI.
    username (str): Account name shown on the Prism Central user control.

  Returns:
    None: The login page is showing again, or Sign Out could not be clicked.
  """
  logger.info("Logging out")
  _watch_and_close_popups(page, 8)
  deadline = time.monotonic() + 12
  menu = None
  while time.monotonic() < deadline and menu is None:
    _close_popup_once(page)
    menu = _find_user_menu(page, username)
    if menu is None:
      page.wait_for_timeout(POLL_MS)
  if menu is None:
    logger.warning(
      "User menu was not found, so Sign Out was not clicked. Header controls: %s",
      ", ".join(_header_control_names(page)) or "(none)",
    )
    return
  logger.info("Opening user menu")
  sign_out = None
  for _attempt in (1, 2):
    _close_popup_once(page)
    menu = _find_user_menu(page, username) or menu
    try:
      menu.click()
    except Exception as err:
      logger.info("User menu click was blocked (%s)", str(err).split("\n")[0][:160])
      _close_popup_once(page)
      continue
    sign_out_deadline = time.monotonic() + 6
    while time.monotonic() < sign_out_deadline and sign_out is None:
      if _close_popup_once(page):
        sign_out = None
        break
      sign_out = _find_sign_out(page)
      if sign_out is None:
        page.wait_for_timeout(POLL_MS)
    if sign_out is not None:
      break
  if sign_out is None:
    logger.warning("Sign Out was not found after the user menu was opened")
    return
  sign_out.click()
  done = time.monotonic() + 20
  confirmed = False
  while time.monotonic() < done:
    if _back_at_login(page):
      logger.info("Logged out")
      return
    if not confirmed:
      confirm = _find_sign_out_confirm(page)
      if confirm is not None:
        logger.info("Confirming sign out")
        try:
          confirm.click()
          confirmed = True
          continue
        except Exception:
          confirmed = False
    if _close_popup_once(page):
      page.wait_for_timeout(400)
      continue
    page.wait_for_timeout(POLL_MS)
  logger.warning(
    "Sign Out was clicked but the login page was not detected (%s)",
    page.url,
  )


def _shrink_video(path):
  """Re-encode a recording to a small silent webm when ffmpeg is installed.

  Args:
    path (str): Playwright webm path.

  Returns:
    str: Path of the smaller file, or the original path when ffmpeg is absent.
  """
  ffmpeg = shutil.which("ffmpeg")
  if not ffmpeg:
    return path
  shrunk = path + ".small.webm"
  try:
    subprocess.run(
      [
        ffmpeg, "-y", "-i", path,
        "-an",
        "-vf", "scale=%s:%s" % (VIDEO_WIDTH, VIDEO_HEIGHT),
        "-c:v", "libvpx",
        "-b:v", "80k",
        "-crf", "40",
        "-deadline", "realtime",
        shrunk,
      ],
      check=True,
      capture_output=True,
      timeout=90,
    )
  except Exception as err:
    logger.warning("Could not shrink screen recording: %s", err)
    return path
  if os.path.isfile(shrunk) and os.path.getsize(shrunk) > 0:
    try:
      os.remove(path)
    except OSError:
      pass
    return shrunk
  return path


def _save_failure_recording(video, ip):
  """Keep a small webm for a failed login and return its DB payload.

  The watchable file is written under ``logs/prism_ui_login``. The base64
  form is what the local_cli collector stores in ``status_data``.

  Args:
    video: Playwright video object. The browser context must already be closed.
    ip (str): PC or PE address, used as the file name.

  Returns:
    tuple: ``(base64_text, saved_path)``. Either value is "" on failure.
  """
  if video is None:
    return "", ""
  folder = ""
  try:
    path = _shrink_video(video.path())
    folder = os.path.dirname(path)
    safe_ip = re.sub(r"[^A-Za-z0-9._-]", "_", ip or "unknown")
    dest_dir = os.path.join(_BASE_DIR, "logs", "prism_ui_login")
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, "%s.webm" % safe_ip)
    shutil.copyfile(path, dest)
    with open(dest, "rb") as handle:
      encoded = base64.b64encode(handle.read()).decode("ascii")
    logger.info("Failure recording saved to %s", dest)
    return encoded, dest
  except Exception as err:
    logger.warning("Could not read screen recording: %s", err)
    return "", ""
  finally:
    if folder:
      shutil.rmtree(folder, ignore_errors=True)


def _first_visible_now(page, selectors):
  """Return a visible match, or None when nothing is showing yet.

  Args:
    page: Playwright page.
    selectors (list): Selector strings tried as one OR locator.

  Returns:
    Locator or None: The first visible match.
  """
  locator = page.locator(_selector(selectors))
  count = min(locator.count(), 20)
  for index in range(count):
    item = locator.nth(index)
    try:
      if item.is_visible():
        return item
    except Exception:
      continue
  return None


def _visible_named(locator):
  """Return the first visible match from a role or text locator.

  Args:
    locator: Playwright locator that may match several elements.

  Returns:
    Locator or None: The first visible match.
  """
  try:
    count = min(locator.count(), 6)
  except Exception:
    return None
  for index in range(count):
    item = locator.nth(index)
    try:
      if item.is_visible():
        return item
    except Exception:
      continue
  return None


def _welcome_accept_button(page):
  """Return the welcome-banner Accept button when that banner is open.

  PE shows a green control labeled "Accept terms and conditions" over the
  login form. PC uses a shorter Accept label inside the banner dialog.

  Args:
    page: Playwright page on the Prism login screen.

  Returns:
    Locator or None: The button that dismisses the banner.
  """
  button = _first_visible_now(page, WELCOME_ACCEPT_SELECTORS)
  if button is not None:
    return button
  terms = re.compile(r"accept terms( and conditions)?", re.I)
  exact_terms = re.compile(r"^\s*accept terms( and conditions)?\s*$", re.I)
  for locator in (
    page.get_by_role("button", name=terms),
    page.locator("button, a, [role='button'], .ntnx-button").filter(has_text=terms),
    page.get_by_text(exact_terms),
  ):
    found = _visible_named(locator)
    if found is not None:
      return found
  root = _first_visible_now(page, WELCOME_ROOT_SELECTORS)
  if root is not None:
    named = root.get_by_role(
      "button", name=re.compile(r"accept|acknowledge|agree|ok", re.I)
    )
    found = _visible_named(named)
    if found is not None:
      return found
  try:
    dialog = page.get_by_role("dialog").filter(
      has_text=re.compile(r"welcome", re.I)
    )
    if dialog.count() and dialog.first.is_visible():
      named = dialog.first.get_by_role(
        "button", name=re.compile(r"accept|acknowledge|agree|ok", re.I)
      )
      found = _visible_named(named)
      if found is not None:
        return found
  except Exception:
    return None
  return None


def _dismiss_welcome_banner(page):
  """Accept a pre-login welcome banner on PE or PC when Prism shows one.

  The PE banner is drawn on top of the login form, so a visible username
  field does not mean the banner is gone.

  Args:
    page: Playwright page after the Prism URL has loaded.

  Returns:
    None: The banner is dismissed, or there was no banner.
  """
  deadline = time.monotonic() + 8
  form_seen_at = None
  while time.monotonic() < deadline:
    button = _welcome_accept_button(page)
    if button is not None:
      logger.info("Accepting welcome banner")
      try:
        button.click(timeout=5000)
      except Exception:
        button.click(force=True, timeout=5000)
      page.wait_for_timeout(800)
      if _welcome_accept_button(page) is None:
        return
      continue
    if _password_visible(page) or _first_visible_now(page, USERNAME_SELECTORS):
      form_seen_at = form_seen_at or time.monotonic()
      if time.monotonic() - form_seen_at >= 2:
        return
    page.wait_for_timeout(POLL_MS)


def _click_local_account_link(page):
  """Click the Nutanix local-account link when Active Directory is offered.

  Args:
    page: Playwright page on a Prism Central login screen.

  Returns:
    bool: True when the local-account control was clicked.
  """
  link = _first_visible_now(page, LOCAL_ACCOUNT_SELECTORS)
  if link is None:
    for role in ("link", "button"):
      candidate = page.get_by_role(
        role, name=re.compile(r"nutanix local account", re.I)
      )
      try:
        if candidate.count() and candidate.first.is_visible():
          link = candidate.first
          break
      except Exception:
        continue
  if link is None:
    return False
  logger.info("Selecting Nutanix local account")
  link.click()
  page.wait_for_timeout(800)
  return True


def _select_local_provider(page):
  """Choose the local directory when a provider dropdown lists Active Directory.

  Args:
    page: Playwright page on a Prism Central login screen.

  Returns:
    bool: True when the dropdown selection was changed to the local account.
  """
  dropdown = _first_visible_now(page, PROVIDER_SELECTORS)
  if dropdown is None:
    return False
  options = dropdown.locator("option")
  try:
    count = options.count()
  except Exception:
    return False
  chosen = None
  for index in range(count):
    option = options.nth(index)
    text = " ".join((option.inner_text() or "").split()).lower()
    value = (option.get_attribute("value") or "").strip()
    if "local" in text or value.lower() in {"local", "nutanix"}:
      chosen = value
      break
  if not chosen:
    return False
  try:
    if dropdown.input_value() == chosen:
      return False
  except Exception:
    pass
  logger.info("Selecting local directory provider")
  dropdown.select_option(value=chosen)
  page.wait_for_timeout(500)
  return True


def _use_nutanix_local_account(page):
  """Switch Prism Central to the local account when Active Directory is configured.

  Args:
    page: Playwright page after any welcome banner has been accepted.

  Returns:
    None: The local login form is selected, or the page already shows one.
  """
  deadline = time.monotonic() + 8
  form_seen_at = None
  while time.monotonic() < deadline:
    if _click_local_account_link(page) or _select_local_provider(page):
      return
    if _password_visible(page):
      form_seen_at = form_seen_at or time.monotonic()
      if time.monotonic() - form_seen_at >= 3:
        return
    page.wait_for_timeout(POLL_MS)


def login_prism(browser, ip, username, password, pause_ms=0, use_local_account=False):
  """Open one Prism UI and submit the login form.

  Args:
    browser: A running Playwright Chromium browser.
    ip (str): Prism Central or Prism Element virtual IP.
    username (str): Prism username.
    password (str): Prism password.
    pause_ms (int): How long to leave the page open after the attempt.
      Used when the browser window is visible.
    use_local_account (bool): When True, choose the Nutanix local account
      if Prism Central is offering Active Directory.

  Returns:
    dict: login_ok, error_message, url, and screen_record.
      screen_record is set only when the login fails.
  """
  record_dir = tempfile.mkdtemp(prefix="czmon-prism-")
  outcome = {
    "login_ok": False,
    "error_message": "",
    "url": "",
    "screen_record": "",
    "screen_record_file": "",
  }
  context = browser.new_context(
    ignore_https_errors=True,
    record_video_dir=record_dir,
    record_video_size={"width": VIDEO_WIDTH, "height": VIDEO_HEIGHT},
  )
  page = context.new_page()
  url = f"https://{ip}:9440/"
  video = None
  try:
    page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    _dismiss_welcome_banner(page)
    if use_local_account:
      _use_nutanix_local_account(page)
    username_box = _first_visible(page, USERNAME_SELECTORS, FORM_TIMEOUT_MS)
    password_box = _first_visible(page, PASSWORD_SELECTORS, FORM_TIMEOUT_MS)
    username_box.fill(username)
    password_box.fill(password)
    try:
      _first_visible(page, SUBMIT_SELECTORS, 5000).click()
    except PlaywrightTimeoutError:
      password_box.press("Enter")
    login_ok, error_message = wait_for_login_result(page)
    outcome["login_ok"] = login_ok
    outcome["error_message"] = error_message
    outcome["url"] = page.url
    if login_ok:
      try:
        _logout(page, username)
      except Exception as err:
        logger.warning("Logout failed: %s", err)
  except PlaywrightTimeoutError as err:
    outcome["error_message"] = (
      f"Timed out opening {url}: {err}".split("\n")[0][:300]
    )
    try:
      outcome["url"] = page.url
    except Exception:
      outcome["url"] = ""
  except Exception as err:
    outcome["error_message"] = str(err).split("\n")[0][:300]
  finally:
    if pause_ms:
      try:
        page.wait_for_timeout(pause_ms)
      except Exception:
        pass
    try:
      video = page.video
    except Exception:
      video = None
    context.close()
    if not outcome["login_ok"]:
      encoded, saved = _save_failure_recording(video, ip)
      outcome["screen_record"] = encoded
      outcome["screen_record_file"] = saved
    shutil.rmtree(record_dir, ignore_errors=True)
  return outcome


def _login_result(login_ok, error_message, screen_record="", screen_record_file=""):
  """Build the JSON object stored for one PC or PE.

  Args:
    login_ok (bool): True when the Prism UI accepted the login.
    error_message (str): Message shown by the UI, or a timeout/connection reason.
    screen_record (str): Base64 webm of a failed attempt. Omitted on success.
    screen_record_file (str): Path of the webm on disk. Omitted on success.

  Returns:
    dict: ``{"login_attempt": true, "error": ""}`` on success, or the failure
      object with ``Error`` and, when captured, the recording.
  """
  if login_ok:
    return {"login_attempt": True, "error": ""}
  result = {"login_attempt": False, "Error": error_message or "Login failed"}
  if screen_record_file:
    result["screen_record_file"] = screen_record_file
  if screen_record:
    result["screen_record"] = screen_record
  return result


def run_prism_ui_login(config_path=None, headed=False, slow_mo=0):
  """Log in to every configured PC and PE and print JSON for the database.

  Args:
    config_path (str, optional): Path to endpoints.json.
    headed (bool): When True, open a visible Chromium window for each login.
    slow_mo (int): Milliseconds to pause between browser actions. Applied
      only when ``headed`` is True.

  Returns:
    None: Prints a JSON object keyed by IP to stdout. LocalProcessor stores
      each value in the ``check_prism_ui_login`` table.
  """
  path = resolve_config_path(config_path)
  endpoints = _usable_rows(path)
  if not endpoints:
    logger.error("No PC or PE IP addresses found in %s", path)
    sys.exit(1)

  logger.info("Using endpoints from %s", path)
  if headed:
    logger.info("Chromium window is visible. Each PC and PE opens in order.")
  results = {}
  pause_ms = 4000 if headed else 0
  with sync_playwright() as playwright:
    browser = playwright.chromium.launch(
      headless=not headed,
      slow_mo=slow_mo if headed else 0,
    )
    try:
      for endpoint in endpoints:
        ip = endpoint["ip"]
        kind = endpoint.get("type") or "UNKNOWN"
        name = endpoint.get("name") or ""
        username, password = _credentials(endpoint)
        logger.info("Logging in to %s %s (%s)", kind, name or ip, ip)
        outcome = login_prism(
          browser,
          ip,
          username,
          password,
          pause_ms=pause_ms,
          use_local_account=(kind == "PC"),
        )
        results[ip] = _login_result(
          bool(outcome["login_ok"]),
          outcome.get("error_message") or "",
          outcome.get("screen_record") or "",
          outcome.get("screen_record_file") or "",
        )
        summary = {
          "login_attempt": results[ip]["login_attempt"],
          "error": results[ip].get("error", results[ip].get("Error", "")),
        }
        if results[ip].get("screen_record_file"):
          summary["screen_record_file"] = results[ip]["screen_record_file"]
        logger.info("%s %s (%s): %s", kind, name or ip, ip, summary)
    finally:
      browser.close()

  printable = results
  if sys.stdout.isatty():
    printable = {}
    for ip, row in results.items():
      shown = dict(row)
      if shown.get("screen_record"):
        shown["screen_record"] = (
          "<base64 webm omitted from the terminal; open screen_record_file>"
        )
      printable[ip] = shown
  print(json.dumps(printable, indent=2))


def _parse_args(argv=None):
  """Parse the optional config path and the visible-browser flag.

  Args:
    argv (list, optional): Arguments excluding the program name.

  Returns:
    argparse.Namespace: ``config``, ``headed``, and ``slow_mo``.
  """
  parser = argparse.ArgumentParser(
    description="Log in to every configured Prism Central and Prism Element UI."
  )
  parser.add_argument(
    "config",
    nargs="?",
    default=None,
    help="Path to endpoints.json. Defaults to the configured endpoints file.",
  )
  parser.add_argument(
    "--headed",
    action="store_true",
    help="Show the Chromium window while each PC and PE login runs.",
  )
  parser.add_argument(
    "--slow-mo",
    type=int,
    default=400,
    help="Milliseconds between browser actions when --headed is set.",
  )
  return parser.parse_args(argv)


if __name__ == "__main__":
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    stream=sys.stderr,
  )
  args = _parse_args()
  run_prism_ui_login(args.config, headed=args.headed, slow_mo=args.slow_mo)
