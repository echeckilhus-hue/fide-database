#!/usr/bin/env python3
"""
process_fide.py
---------------
Télécharge la liste officielle FIDE (XML), la parse et crée
un SQLite optimisé pour la recherche dans l'app Electron.

Sortie : fide_players.db (SQLite) + fide_players.db.gz (compressé)

Format FIDE XML :
  <player>
    <fideid>1234567</fideid>
    <name>CARLSEN, Magnus</name>
    <country>NOR</country>
    <sex>M</sex>
    <title>GM</title>
    <rating>2831</rating>
    <rapid_rating>2828</rapid_rating>
    <blitz_rating>2886</blitz_rating>
    <birthday>1990</birthday>
  </player>
"""

import os
import sys
import gzip
import shutil
import sqlite3
import zipfile
import hashlib
import datetime
import xml.etree.ElementTree as ET

# ── Configuration ──────────────────────────────────────────────────────────────
FIDE_DOWNLOAD_URL   = "https://ratings.fide.com/download/players_list_xml_foa.zip"
FIDE_RAPID_URL      = "https://ratings.fide.com/download/players_list_xml_rapid_foa.zip"
FIDE_BLITZ_URL      = "https://ratings.fide.com/download/players_list_xml_blitz_foa.zip"
OUTPUT_DB           = "fide_players.db"
OUTPUT_GZ           = "fide_players.db.gz"
BATCH_SIZE          = 5_000

# ── Helpers ────────────────────────────────────────────────────────────────────
def log(msg: str) -> None:
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def download_fide_zip(url: str, dest: str, max_tries: int = 5) -> str:
    """Télécharge un zip FIDE et retourne le nom du XML extrait.

    Réessaie avec un backoff croissant en cas de coupure réseau : le
    serveur FIDE timeout parfois depuis les runners GitHub (congestion
    côté FIDE au moment de la régénération mensuelle, ou filtrage des
    IP datacenter - cause encore incertaine, le retry couvre les deux).
    """
    import urllib.request
    import urllib.error
    import time

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    }

    last_error = None
    for attempt in range(1, max_tries + 1):
        try:
            log(f"📥 Téléchargement depuis {url} (tentative {attempt}/{max_tries})")
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=120) as resp:
                total = int(resp.headers.get("Content-Length", 0))
                downloaded = 0
                with open(dest + ".zip", "wb") as f:
                    while True:
                        chunk = resp.read(65536)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total:
                            pct = downloaded / total * 100
                            print(f"\r  {downloaded / 1024 / 1024:.1f} MB / {total / 1024 / 1024:.1f} MB ({pct:.0f}%)", end="", flush=True)
            print()

            if total and downloaded < total:
                raise IOError(f"Téléchargement incomplet ({downloaded} / {total} octets)")

            last_error = None
            break

        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            last_error = e
            log(f"❌ Erreur téléchargement (tentative {attempt}/{max_tries}) : {e}")
            if os.path.exists(dest + ".zip"):
                os.remove(dest + ".zip")
            if attempt < max_tries:
                wait = min(30 * (2 ** (attempt - 1)), 300)
                log(f"⏳ Nouvelle tentative dans {wait}s...")
                time.sleep(wait)

    if last_error is not None:
        raise RuntimeError(f"Échec du téléchargement après {max_tries} tentatives : {last_error}") from last_error

    log("📦 Extraction du ZIP...")
    with zipfile.ZipFile(dest + ".zip", "r") as z:
        xml_names = [n for n in z.namelist() if n.lower().endswith(".xml")]
        if not xml_names:
            raise RuntimeError("Aucun fichier XML dans le ZIP FIDE")
        xml_name = xml_names[0]
        z.extract(xml_name, ".")
        os.rename(xml_name, dest)

    os.remove(dest + ".zip")
    size_mb = os.path.getsize(dest) / 1024 / 1024
    log(f"✓ Extrait : {dest} ({size_mb:.1f} MB)")
    return dest


def safe_int(val, default=0):
    """Conversion sûre en entier."""
    try:
        return int(val) if val and str(val).strip() else default
    except (ValueError, TypeError):
        return default


def parse_and_insert(xml_path: str, conn: sqlite3.Connection, rating_field: str = "rating",
                     rapid_field: str = "rapid_rating", blitz_field: str = "blitz_rating") -> int:
    """
    Parse le XML FIDE et insère dans la DB SQLite.
    Retourne le nombre de joueurs traités.
    """
    cur = conn.cursor()
    count = 0
    batch = []

    log(f"⚙️  Parsing {xml_path}...")
    
    for event, elem in ET.iterparse(xml_path, events=("end",)):
        if elem.tag != "player":
            continue

        fide_id = safe_int(elem.findtext("fideid"))
        name    = (elem.findtext("name") or "").strip()
        fed     = (elem.findtext("country") or "").strip().upper()
        sex     = (elem.findtext("sex") or "").strip().upper()
        title   = (elem.findtext("title") or "").strip()
        elo     = safe_int(elem.findtext(rating_field))
        rapid   = safe_int(elem.findtext(rapid_field))
        blitz   = safe_int(elem.findtext(blitz_field))
        birth   = (elem.findtext("birthday") or "").strip()

        if fide_id and name:
            batch.append((fide_id, name, fed, sex, title, elo, rapid, blitz, birth))
            count += 1

            if len(batch) >= BATCH_SIZE:
                cur.executemany(
                    "INSERT OR REPLACE INTO players VALUES (?,?,?,?,?,?,?,?,?)",
                    batch
                )
                conn.commit()
                batch = []
                print(f"\r  {count:,} joueurs traités...", end="", flush=True)

        elem.clear()

    if batch:
        cur.executemany(
            "INSERT OR REPLACE INTO players VALUES (?,?,?,?,?,?,?,?,?)",
            batch
        )
        conn.commit()

    print()
    return count


def create_db(path: str) -> sqlite3.Connection:
    """Crée la base SQLite avec le schéma optimisé."""
    if os.path.exists(path):
        os.remove(path)
    
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
    
    conn.execute("""
        CREATE TABLE players (
            fide_id  INTEGER PRIMARY KEY,
            name     TEXT NOT NULL,
            fed      TEXT,
            sex      TEXT,
            title    TEXT,
            elo      INTEGER DEFAULT 0,
            rapid    INTEGER DEFAULT 0,
            blitz    INTEGER DEFAULT 0,
            birth    TEXT
        )
    """)
    # Index full-text simplifié (compatible sans FTS5)
    conn.execute("CREATE INDEX idx_name ON players(name COLLATE NOCASE)")
    conn.execute("CREATE INDEX idx_fed  ON players(fed)")
    conn.execute("CREATE INDEX idx_elo  ON players(elo DESC) WHERE elo > 0")
    conn.commit()
    return conn


def compress_db(db_path: str, gz_path: str) -> None:
    """Compresse le SQLite en gzip."""
    log(f"🗜️  Compression → {gz_path}")
    with open(db_path, "rb") as f_in:
        with gzip.open(gz_path, "wb", compresslevel=9) as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    size_in  = os.path.getsize(db_path)  / 1024 / 1024
    size_out = os.path.getsize(gz_path)  / 1024 / 1024
    ratio    = (1 - size_out / size_in) * 100
    log(f"✓ {size_in:.1f} MB → {size_out:.1f} MB (compression {ratio:.0f}%)")


def checksum(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    start = datetime.datetime.now()
    log("🌍 === FIDE Database Builder ===")
    log(f"Date : {start.strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. Télécharger le XML standard (contient elo, rapid, blitz)
    download_fide_zip(FIDE_DOWNLOAD_URL, "fide_standard.xml")

    # 2. Créer la DB
    log("🗄️  Création de la base SQLite...")
    conn = create_db(OUTPUT_DB)

    # 3. Parser et insérer
    count = parse_and_insert(
        "fide_standard.xml", conn,
        rating_field="rating",
        rapid_field="rapid_rating",
        blitz_field="blitz_rating"
    )
    
    log(f"✓ {count:,} joueurs insérés")

    # 4. Statistiques par fédération (top 10)
    log("📊 Top 10 fédérations :")
    cur = conn.cursor()
    top_feds = cur.execute("""
        SELECT fed, COUNT(*) as n, AVG(elo) as avg_elo
        FROM players WHERE fed != ''
        GROUP BY fed ORDER BY n DESC LIMIT 10
    """).fetchall()
    for fed, n, avg in top_feds:
        print(f"   {fed:5s}: {n:8,} joueurs  (elo moyen: {avg:.0f})")

    # 5. Vérification intégrité
    total = cur.execute("SELECT COUNT(*) FROM players").fetchone()[0]
    rated = cur.execute("SELECT COUNT(*) FROM players WHERE elo > 0").fetchone()[0]
    with_fide = cur.execute("SELECT COUNT(*) FROM players WHERE fide_id > 0").fetchone()[0]
    log(f"✓ Vérification: {total:,} total, {rated:,} classés, {with_fide:,} avec ID FIDE")

    conn.close()

    # 6. Optimisation SQLite (VACUUM pour compacter)
    log("⚙️  Optimisation (VACUUM)...")
    conn2 = sqlite3.connect(OUTPUT_DB)
    conn2.execute("VACUUM")
    conn2.close()

    # 7. Compression
    compress_db(OUTPUT_DB, OUTPUT_GZ)

    # 8. Checksum
    sha = checksum(OUTPUT_GZ)
    log(f"🔐 SHA-256: {sha[:16]}...{sha[-8:]}")

    # 9. Nettoyage
    for f in ["fide_standard.xml", "fide_rapid.xml", "fide_blitz.xml"]:
        if os.path.exists(f):
            os.remove(f)

    # 10. Résumé final
    duration = (datetime.datetime.now() - start).total_seconds()
    log("=" * 50)
    log(f"✅ Terminé en {duration:.0f}s")
    log(f"   📁 {OUTPUT_DB}  : {os.path.getsize(OUTPUT_DB)/1024/1024:.1f} MB")
    log(f"   📦 {OUTPUT_GZ} : {os.path.getsize(OUTPUT_GZ)/1024/1024:.1f} MB")
    log(f"   👥 Joueurs   : {total:,}")
    log(f"   📅 Date      : {start.strftime('%Y-%m')}")

    # Pour GitHub Actions summary
    summary = f"""
## 🌍 FIDE Database — {start.strftime('%Y-%m')}

| | |
|---|---|
| 👥 Joueurs total | {total:,} |
| 📊 Joueurs classés | {rated:,} |
| 📦 Taille compressée | {os.path.getsize(OUTPUT_GZ)/1024/1024:.1f} MB |
| ⏱️ Durée | {duration:.0f}s |
"""
    if "GITHUB_STEP_SUMMARY" in os.environ:
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(summary)


if __name__ == "__main__":
    main()
