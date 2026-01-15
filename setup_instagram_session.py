#!/usr/bin/env python3
"""
Instagram sessie setup script.

Dit script helpt je om een Instagram sessie aan te maken voor hogere rate limits.

STAPPEN:
1. Log in op Instagram in Firefox
2. Voer dit script uit
3. De sessie wordt opgeslagen en kan hergebruikt worden

Bron: https://instaloader.github.io/troubleshooting.html
"""
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

try:
    import instaloader
except ImportError:
    print("FOUT: instaloader is niet geinstalleerd")
    print("Installeer met: pip install instaloader")
    sys.exit(1)


def import_from_firefox():
    """Importeer sessie cookies vanuit Firefox."""
    print("=" * 60)
    print("INSTAGRAM SESSIE IMPORTEREN VANUIT FIREFOX")
    print("=" * 60)
    print()
    print("STAPPEN:")
    print("1. Open Firefox")
    print("2. Ga naar instagram.com")
    print("3. Log in met je account")
    print("4. Sluit Firefox NIET af")
    print("5. Druk op Enter om verder te gaan...")
    print()
    input("Druk Enter als je bent ingelogd in Firefox...")

    try:
        # Probeer Firefox cookies te importeren
        from sqlite3 import connect
        from shutil import copy2
        from tempfile import TemporaryDirectory

        # Zoek Firefox profiles
        if sys.platform == "win32":
            firefox_dir = Path(os.environ["APPDATA"]) / "Mozilla" / "Firefox" / "Profiles"
        else:
            firefox_dir = Path.home() / ".mozilla" / "firefox"

        if not firefox_dir.exists():
            print(f"FOUT: Firefox profiles niet gevonden in {firefox_dir}")
            return None

        # Zoek profile met cookies
        cookies_db = None
        for profile in firefox_dir.iterdir():
            if profile.is_dir():
                potential = profile / "cookies.sqlite"
                if potential.exists():
                    cookies_db = potential
                    break

        if not cookies_db:
            print("FOUT: Geen cookies.sqlite gevonden")
            return None

        print(f"Cookies gevonden: {cookies_db}")

        # Kopieer database (kan locked zijn)
        with TemporaryDirectory() as tmpdir:
            tmp_db = Path(tmpdir) / "cookies.sqlite"
            copy2(cookies_db, tmp_db)

            conn = connect(tmp_db)
            cursor = conn.cursor()

            # Haal Instagram cookies op
            cursor.execute("""
                SELECT name, value FROM moz_cookies
                WHERE host LIKE '%instagram.com'
            """)

            cookies = {row[0]: row[1] for row in cursor.fetchall()}
            conn.close()

            if not cookies:
                print("FOUT: Geen Instagram cookies gevonden")
                print("Zorg dat je bent ingelogd in Firefox")
                return None

            print(f"Gevonden: {len(cookies)} Instagram cookies")
            return cookies

    except Exception as e:
        print(f"FOUT bij importeren: {e}")
        return None


def create_session_with_cookies(cookies: dict, username: str):
    """Maak instaloader sessie met cookies."""
    try:
        loader = instaloader.Instaloader(max_connection_attempts=1)

        # Update session cookies
        loader.context._session.cookies.update(cookies)

        # Test login
        test_user = loader.test_login()
        if test_user:
            print(f"Sessie succesvol! Ingelogd als: {test_user}")

            # Sla sessie op
            session_file = Path(__file__).parent / "data" / f"session-{test_user}"
            session_file.parent.mkdir(exist_ok=True)

            loader.save_session_to_file(str(session_file))
            print(f"Sessie opgeslagen: {session_file}")
            return str(session_file), test_user
        else:
            print("FOUT: Login test mislukt")
            return None, None

    except Exception as e:
        print(f"FOUT bij sessie aanmaken: {e}")
        return None, None


def login_interactive():
    """Interactieve login via command line."""
    print("=" * 60)
    print("INSTAGRAM INTERACTIEVE LOGIN")
    print("=" * 60)
    print()
    print("Let op: 2FA moet mogelijk handmatig ingevoerd worden")
    print()

    username = input("Instagram username: ").strip()
    if not username:
        print("Geen username opgegeven")
        return None, None

    try:
        loader = instaloader.Instaloader()
        loader.login(username, input("Wachtwoord: "))

        # Sla sessie op
        session_file = Path(__file__).parent / "data" / f"session-{username}"
        session_file.parent.mkdir(exist_ok=True)

        loader.save_session_to_file(str(session_file))
        print(f"\nSessie opgeslagen: {session_file}")
        return str(session_file), username

    except Exception as e:
        print(f"FOUT bij login: {e}")
        return None, None


def main():
    print("=" * 60)
    print("INSTAGRAM SESSIE SETUP")
    print("=" * 60)
    print()
    print("Een ingelogde sessie geeft hogere rate limits voor dataverzameling.")
    print()
    print("Kies een methode:")
    print("1. Importeer vanuit Firefox (aanbevolen)")
    print("2. Interactieve login")
    print("3. Annuleren")
    print()

    choice = input("Keuze (1/2/3): ").strip()

    if choice == "1":
        cookies = import_from_firefox()
        if cookies:
            username = input("\nJe Instagram username: ").strip()
            session_file, user = create_session_with_cookies(cookies, username)
            if session_file:
                print("\n" + "=" * 60)
                print("SUCCES!")
                print("=" * 60)
                print(f"\nGebruik nu het collectie script met:")
                print(f"  --session {session_file}")
                print(f"  --username {user}")

    elif choice == "2":
        session_file, username = login_interactive()
        if session_file:
            print("\n" + "=" * 60)
            print("SUCCES!")
            print("=" * 60)

    else:
        print("Geannuleerd")


if __name__ == "__main__":
    main()
