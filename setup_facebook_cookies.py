#!/usr/bin/env python3
"""
Helper script om Facebook cookies te exporteren voor de scraper.

STAPPEN:
1. Open Firefox of Chrome
2. Ga naar facebook.com en log in
3. Installeer "EditThisCookie" (Chrome) of "Cookie Quick Manager" (Firefox)
4. Exporteer cookies naar JSON of Netscape formaat
5. Sla op als 'facebook_cookies.txt' in deze map
6. Voer dit script uit om te testen

Of: sluit Chrome volledig en voer dit script uit om automatisch cookies te laden.
"""
import sys
import os
from pathlib import Path

# Fix Windows encoding
os.environ['PYTHONIOENCODING'] = 'utf-8'
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

COOKIES_FILE = Path(__file__).parent / "facebook_cookies.txt"


def try_chrome_cookies():
    """Probeer cookies uit Chrome te laden (Chrome moet gesloten zijn)."""
    print("Proberen cookies uit Chrome te laden...")
    print("(Chrome moet volledig gesloten zijn)")
    print()

    try:
        import browser_cookie3
        cookies = browser_cookie3.chrome(domain_name='.facebook.com')

        cookie_dict = {}
        for cookie in cookies:
            cookie_dict[cookie.name] = cookie.value

        if 'c_user' in cookie_dict and 'xs' in cookie_dict:
            print("SUCCESS! Facebook cookies gevonden:")
            print(f"  c_user: {cookie_dict['c_user'][:10]}...")
            print(f"  xs: {cookie_dict['xs'][:10]}...")

            # Sla op in Netscape formaat
            with open(COOKIES_FILE, 'w') as f:
                f.write("# Netscape HTTP Cookie File\n")
                f.write("# https://curl.haxx.se/rfc/cookie_spec.html\n")
                f.write("# This is a generated file! Do not edit.\n\n")
                for cookie in cookies:
                    expires = str(int(cookie.expires)) if cookie.expires else "0"
                    secure = "TRUE" if cookie.secure else "FALSE"
                    f.write(f".facebook.com\tTRUE\t/\t{secure}\t{expires}\t{cookie.name}\t{cookie.value}\n")

            print(f"\nCookies opgeslagen in: {COOKIES_FILE}")
            return True
        else:
            print("FOUT: c_user of xs cookie niet gevonden")
            print("Ben je ingelogd op Facebook in Chrome?")
            return False

    except Exception as e:
        print(f"FOUT: {e}")
        print()
        print("Mogelijke oorzaken:")
        print("  - Chrome is nog open (sluit Chrome volledig)")
        print("  - Niet ingelogd op Facebook")
        print("  - Windows beveiligingsrestricties")
        return False


def try_firefox_cookies():
    """Probeer cookies uit Firefox te laden."""
    print("\nProberen cookies uit Firefox te laden...")

    try:
        import browser_cookie3
        cookies = browser_cookie3.firefox(domain_name='.facebook.com')

        cookie_dict = {}
        for cookie in cookies:
            cookie_dict[cookie.name] = cookie.value

        if 'c_user' in cookie_dict and 'xs' in cookie_dict:
            print("SUCCESS! Facebook cookies gevonden in Firefox")

            with open(COOKIES_FILE, 'w') as f:
                f.write("# Netscape HTTP Cookie File\n")
                for cookie in cookies:
                    expires = str(int(cookie.expires)) if cookie.expires else "0"
                    secure = "TRUE" if cookie.secure else "FALSE"
                    f.write(f".facebook.com\tTRUE\t/\t{secure}\t{expires}\t{cookie.name}\t{cookie.value}\n")

            print(f"Cookies opgeslagen in: {COOKIES_FILE}")
            return True
        else:
            print("FOUT: Niet ingelogd op Facebook in Firefox")
            return False

    except Exception as e:
        print(f"Firefox niet gevonden of niet beschikbaar: {e}")
        return False


def check_existing_cookies():
    """Check of er al een cookies bestand is."""
    if COOKIES_FILE.exists():
        print(f"Cookies bestand gevonden: {COOKIES_FILE}")
        with open(COOKIES_FILE, 'r') as f:
            content = f.read()
            if 'c_user' in content and 'xs' in content:
                print("Cookies lijken geldig (c_user en xs aanwezig)")
                return True
            else:
                print("Cookies bestand ongeldig - c_user of xs ontbreekt")
                return False
    return False


def manual_instructions():
    """Instructies voor handmatig exporteren."""
    print()
    print("=" * 60)
    print("HANDMATIGE EXPORT INSTRUCTIES")
    print("=" * 60)
    print()
    print("Als automatisch laden niet werkt, exporteer handmatig:")
    print()
    print("CHROME:")
    print("1. Installeer 'EditThisCookie' extensie")
    print("2. Ga naar facebook.com en log in")
    print("3. Klik op EditThisCookie icoon")
    print("4. Klik 'Export' (kopieer naar klembord)")
    print("5. Maak bestand: facebook_cookies.txt")
    print("6. Plak de JSON inhoud")
    print()
    print("FIREFOX:")
    print("1. Installeer 'Cookie Quick Manager' addon")
    print("2. Ga naar facebook.com en log in")
    print("3. Open Cookie Quick Manager")
    print("4. Filter op 'facebook.com'")
    print("5. Export als Netscape formaat")
    print("6. Sla op als: facebook_cookies.txt")
    print()
    print(f"Pad: {COOKIES_FILE}")


def main():
    print("=" * 60)
    print("FACEBOOK COOKIES SETUP")
    print("=" * 60)
    print()

    # Check bestaand bestand
    if check_existing_cookies():
        print("\nGebruik bestaande cookies? (druk Enter om door te gaan, 'n' voor nieuwe)")
        response = input("> ").strip().lower()
        if response != 'n':
            print("\nKlaar! Voer collect_facebook_engagement.py uit")
            return

    # Probeer automatisch
    if try_chrome_cookies():
        print("\nKlaar! Voer collect_facebook_engagement.py uit")
        return

    if try_firefox_cookies():
        print("\nKlaar! Voer collect_facebook_engagement.py uit")
        return

    # Handmatige instructies
    manual_instructions()


if __name__ == "__main__":
    main()
