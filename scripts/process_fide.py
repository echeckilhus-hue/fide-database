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

✅ FIX FIDE-25 (19/11/2026) — LA BASE PUBLIÉE CONTENAIT LA LISTE DU 30/04/2024.
Relevé le 19/11/2026 sur ratings.fide.com : « players_list_xml_foa.zip » n'est plus dans la page des listes
(download_lists.phtml) et sa date est le 30/04/2024 (en-tête Last-Modified) ; chaque mois, ce script republiait donc
la liste du 30/04/2024. La liste en cours est « players_list_xml.zip » (même XML : l'archive contient toujours
« players_list_xml_foa.xml »). DÉSORMAIS :
  • l'adresse est celle de la liste en cours ;
  • la date de la liste (date du fichier dans l'archive de la FIDE) est contrôlée : plus de 45 jours = échec du
    workflow (aucune base périmée publiée) ;
  • la base porte une table « meta » (list_date, source_file, format, count, built_at) : le logiciel affiche la date
    de la liste et refuse une base de secours non datée ou trop ancienne (electron-fide-handlers.js, FIX FIDE-25).
Le logiciel importe d'ailleurs lui-même la liste officielle depuis la livraison 25 : cette base n'est plus qu'un secours.
⚠️ HISTORIQUE (à conserver) : FIDE_DOWNLOAD_URL = "https://ratings.fide.com/download/players_list_xml_foa.zip"
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
FIDE_DOWNLOAD_URL   = "https://ratings.fide.com/download/players_list_xml.zip"   # ✅ FIX FIDE-25
OUTPUT_DB           = "fide_players.db"
OUTPUT_GZ           = "fide_players.db.gz"
BATCH_SIZE          = 5_000
AGE_MAX_JOURS       = 45   # ✅ FIX FIDE-25 : une liste plus ancienne n'est pas publiée

# ── Helpers ────────────────────────────────────────────────────────────────────
def log(msg: str) -> None:
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def download_fide_zip(url: str, dest: str):
    """Télécharge un zip FIDE, extrait le XML dans dest ; retourne (dest, date de la liste AAAA-MM-JJ)."""
    import urllib.request
    import urllib.error

    log(f"📥 Téléchargement depuis {url}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "TournamentManager/1.0"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            total = int(resp.headers.get("Content-Length", 0))
            log(f"   Last-Modified : {resp.headers.get('Last-Modified', '?')}")
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
        if total and downloaded != total:
            raise RuntimeError(f"Téléchargement incomplet : {downloaded} octets sur {total}")
    except (urllib.error.URLError, TimeoutError) as e:
        log(f"❌ Erreur téléchargement : {e}")
        raise

    log("📦 Extraction du ZIP...")
    with zipfile.ZipFile(dest + ".zip", "r") as z:
        infos = [i for i in z.infolist() if i.filename.lower().endswith(".xml")]
        if not infos:
            raise RuntimeError("Aucun fichier XML dans le ZIP FIDE")
        info = infos[0]
        list_date = "%04d-%02d-%02d" % info.date_time[:3]   # ✅ FIX FIDE-25 : date de la liste
        with z.open(info) as src, open(dest, "wb") as out:
            shutil.copyfileobj(src, out, 1 << 20)

    os.remove(dest + ".zip")
    size_mb = os.path.getsize(dest) / 1024 / 1024
    log(f"✓ Extrait : {dest} ({size_mb:.1f} MB) — liste du {list_date}")
    return dest, list_date


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
    first = True

    log(f"⚙️  Parsing {xml_path}...")

    for event, elem in ET.iterparse(xml_path, events=("end",)):
        if elem.tag != "player":
            continue

        # ✅ FIX FIDE-25 : la liste doit être la liste COMBINÉE (classique, rapide, blitz)
        if first:
            first = False
            if elem.find(rapid_field) is None or elem.find(blitz_field) is None:
                raise RuntimeError("Liste non combinée (Elo rapide ou blitz absents) : ce n'est pas players_list_xml.zip")

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
    # ✅ FIX FIDE-25 : date et provenance de la liste
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
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

    # 1. Télécharger la liste COMBINÉE en cours (classique, rapide, blitz)
    _, list_date = download_fide_zip(FIDE_DOWNLOAD_URL, "fide_standard.xml")

    # ✅ FIX FIDE-25 : jamais de liste périmée publiée
    age = (datetime.date.today() - datetime.date.fromisoformat(list_date)).days
    log(f"📅 Liste FIDE du {list_date} ({age} jour(s))")
    if age > AGE_MAX_JOURS:
        raise RuntimeError(f"Liste FIDE du {list_date} : plus de {AGE_MAX_JOURS} jours — l'adresse {FIDE_DOWNLOAD_URL} ne donne plus la liste en cours")

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
    conn.executemany("INSERT OR REPLACE INTO meta VALUES (?, ?)", [
        ("list_date", list_date), ("source_file", "players_list_xml.zip"), ("format", "XML"),
        ("count", str(count)), ("built_at", datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))])
    conn.commit()

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
    conn2.execute("PRAGMA journal_mode=DELETE")
    conn2.execute("VACUUM")
    conn2.close()

    # 7. Compression
    compress_db(OUTPUT_DB, OUTPUT_GZ)

    # 8. Checksum
    sha = checksum(OUTPUT_GZ)
    log(f"🔐 SHA-256: {sha[:16]}...{sha[-8:]}")

    # 9. Nettoyage
    for f in ["fide_standard.xml"]:
        if os.path.exists(f):
            os.remove(f)

    # 10. Résumé final
    duration = (datetime.datetime.now() - start).total_seconds()
    log("=" * 50)
    log(f"✅ Terminé en {duration:.0f}s")
    log(f"   📁 {OUTPUT_DB}  : {os.path.getsize(OUTPUT_DB)/1024/1024:.1f} MB")
    log(f"   📦 {OUTPUT_GZ} : {os.path.getsize(OUTPUT_GZ)/1024/1024:.1f} MB")
    log(f"   👥 Joueurs   : {total:,}")
    log(f"   📅 Liste FIDE du {list_date}")

    # Pour GitHub Actions summary
    summary = f"""
## 🌍 FIDE Database — liste du {list_date}

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
