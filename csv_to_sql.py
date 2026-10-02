import csv
import os
import sys
import time
from datetime import datetime, timedelta
from itertools import chain

import mariadb
from dotenv import load_dotenv

# Starttijd van het programma (voor "totaal verstreken tijd")
PROGRAM_START = time.perf_counter()

CSV_FOLDER = "csv_data"
PROGRESS_EVERY = 1000
BATCH_SIZE = 1000  # aantal regels per executemany + commit

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

if not DB_USER or DB_PASSWORD is None or not DB_NAME:
    print("Fout: de volgende waarden moeten in .env staan:")
    print("  DB_USER")
    print("  DB_PASSWORD")
    print("  DB_NAME")
    sys.exit(1)


def format_duration(seconds):
    """Zet seconden om naar UU:MM:SS."""
    hours, remainder = divmod(int(seconds), 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def per_minute(count, seconds):
    """Aantal regels per minuut (0 als er nog geen tijd verstreken is)."""
    if seconds <= 0:
        return 0.0
    return count / seconds * 60


def parse_start_datetime(value):
    try:
        return datetime.strptime(value, "%Y%m%dT%H%M%S")
    except ValueError:
        raise ValueError(
            "Ongeldige datum/tijd. Gebruik bijvoorbeeld 20260708T065128."
        )


def sql_identifier(value):
    return "`" + value.replace("`", "``") + "`"


def clean_table_part(value):
    value = value.strip()
    return value if value else "empty"


def make_table_name(vehicle_name, fields):
    if len(fields) < 3:
        raise ValueError("CSV-bestand heeft minder dan 3 velden.")

    return f"{vehicle_name},{clean_table_part(fields[0])},{clean_table_part(fields[2])}"


def is_comment(row):
    return row[0].lstrip().startswith("#")


def connect_database():
    try:
        return mariadb.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
        )
    except mariadb.Error as error:
        print(f"Kan geen verbinding maken met database: {error}")
        sys.exit(1)


def create_table(connection, table_name, field_count):
    columns = ["`timestamp` DATETIME NOT NULL", "`C0` VARCHAR(255)"]
    columns.extend(f"`C{i + 1}` TEXT" for i in range(field_count))

    sql = f"CREATE TABLE IF NOT EXISTS {sql_identifier(table_name)} ({', '.join(columns)})"

    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        connection.commit()
    finally:
        cursor.close()


def open_csv_file(filepath):
    file_handle = None

    try:
        file_handle = open(filepath, "r", encoding="utf-8-sig", newline="")
        file_handle.read(65536)
        file_handle.seek(0)
        return file_handle, csv.reader(file_handle), "utf-8-sig"

    except UnicodeDecodeError:
        if file_handle is not None:
            file_handle.close()

    file_handle = open(filepath, "r", encoding="cp1252", newline="")
    return file_handle, csv.reader(file_handle), "cp1252"


def process_csv_file(
    connection,
    filepath,
    vehicle_name,
    start_datetime,
    file_number,
    total_files,
):
    """Verwerkt één CSV-bestand en geeft een dict met statistieken terug."""
    filename = os.path.basename(filepath)

    print()
    print("=" * 70)
    print(f"[{file_number}/{total_files}] Verwerken: {filename}")
    print("=" * 70)

    file_start = time.perf_counter()

    stats = {
        "file": filename,
        "read": 0,
        "inserted": 0,
        "skipped": 0,
        "seconds": 0.0,
        "status": "OK",
    }

    file_handle = None
    cursor = None

    processed = 0
    inserted = 0
    committed = 0  # aantal regels dat echt definitief in de database staat
    skipped = 0

    try:
        try:
            file_handle, reader, encoding = open_csv_file(filepath)
        except OSError as error:
            print(f"  Kan bestand niet openen: {error}")
            stats["status"] = "Fout (openen)"
            return stats

        print(f"  Encoding: {encoding}")

        first_row = None
        for row in reader:
            if not row or is_comment(row):
                continue
            first_row = row
            break

        if first_row is None:
            print("  Geen data gevonden.")
            stats["status"] = "Geen data"
            return stats

        field_count = len(first_row)

        if field_count < 3:
            print("  Overgeslagen: minder dan 3 CSV-velden.")
            stats["status"] = "Overgeslagen"
            return stats

        print(f"  CSV-velden: {field_count}")

        try:
            table_name = make_table_name(vehicle_name, first_row)
        except ValueError as error:
            print(f"  Fout: {error}")
            stats["status"] = "Fout (tabelnaam)"
            return stats

        print(f"  Tabel: {table_name}")

        try:
            create_table(connection, table_name, field_count)
        except mariadb.Error as error:
            print(f"  Fout bij maken tabel: {error}")
            stats["status"] = "Fout (tabel)"
            return stats

        column_names = ["`timestamp`", "`C0`"]
        column_names.extend(f"`C{i + 1}`" for i in range(field_count))

        placeholder_count = field_count + 2
        placeholders = ", ".join(["?"] * placeholder_count)

        insert_sql = (
            f"INSERT INTO {sql_identifier(table_name)} "
            f"({', '.join(column_names)}) "
            f"VALUES ({placeholders})"
        )

        cursor = connection.cursor()

        # Tijdmeting voor deze file begint hier (na voorbereiding)
        file_start = time.perf_counter()

        batch = []

        def flush_batch():
            """Schrijft de huidige batch in één keer naar de database en commit."""
            nonlocal inserted, committed

            if not batch:
                return

            cursor.executemany(insert_sql, batch)
            connection.commit()

            inserted += len(batch)
            committed = inserted
            batch.clear()

        def process_row(row):
            nonlocal skipped

            if not row or is_comment(row):
                return

            if len(row) != field_count:
                skipped += 1
                print(
                    f"\n  Waarschuwing: regel heeft {len(row)} velden, "
                    f"verwacht {field_count}. Overgeslagen."
                )
                return

            seconds_text = row[1].strip()

            try:
                seconds = float(seconds_text)
            except ValueError:
                skipped += 1
                print(
                    f"\n  Waarschuwing: ongeldige secondenwaarde "
                    f"'{seconds_text}'. Regel overgeslagen."
                )
                return

            timestamp = start_datetime + timedelta(seconds=seconds)

            values = [timestamp.strftime("%Y-%m-%d %H:%M:%S"), vehicle_name]
            values.extend(row)

            if len(values) != placeholder_count:
                raise RuntimeError(
                    f"Interne fout: {len(values)} waarden, "
                    f"maar {placeholder_count} placeholders."
                )

            batch.append(values)

            if len(batch) >= BATCH_SIZE:
                flush_batch()

        for row in chain([first_row], reader):
            processed += 1

            try:
                process_row(row)

            except mariadb.Error as error:
                print(
                    f"\n  Databasefout rond regel {processed} "
                    f"(huidige batch van maximaal {BATCH_SIZE:,} regels "
                    f"is teruggedraaid): {error}"
                )
                connection.rollback()
                inserted = committed
                stats["status"] = "Fout (database)"
                return stats

            except RuntimeError as error:
                print(f"\n  {error}")
                connection.rollback()
                inserted = committed
                stats["status"] = "Fout (intern)"
                return stats

            if processed % PROGRESS_EVERY == 0:
                file_elapsed = time.perf_counter() - file_start
                total_elapsed = time.perf_counter() - PROGRAM_START

                line = (
                    f"  Bezig... {processed:,} regels gelezen | "
                    f"{inserted:,} toegevoegd | "
                    f"totaal {format_duration(total_elapsed)} | "
                    f"{per_minute(processed, file_elapsed):,.0f} gelezen/min | "
                    f"{per_minute(inserted, file_elapsed):,.0f} upload/min"
                )
                # ljust voorkomt restjes van een eerdere, langere regel
                print("\r" + line.ljust(120), end="", flush=True)

        # Resterende regels (minder dan BATCH_SIZE) wegschrijven
        flush_batch()

        file_elapsed = time.perf_counter() - file_start

        print()
        print(
            f"  Klaar: {inserted:,} regels toegevoegd "
            f"in {format_duration(file_elapsed)} "
            f"({per_minute(inserted, file_elapsed):,.0f} regels/min)."
        )

        if skipped > 0:
            print(f"  Overgeslagen: {skipped:,} regels.")

        return stats

    except UnicodeDecodeError as error:
        print()
        print(f"  Encodingfout in {filename}: {error}")
        stats["status"] = "Fout (encoding)"
        return stats

    except OSError as error:
        print()
        print(f"  Leesfout in {filename}: {error}")
        stats["status"] = "Fout (lezen)"
        return stats

    except mariadb.Error as error:
        print()
        print(f"  Databasefout bij {filename}: {error}")
        try:
            connection.rollback()
        except Exception:
            pass
        inserted = committed
        stats["status"] = "Fout (database)"
        return stats

    finally:
        if cursor is not None:
            cursor.close()
        if file_handle is not None:
            file_handle.close()

        # Statistieken vullen (ook bij fouten en vroege returns)
        stats["read"] = processed
        stats["inserted"] = inserted
        stats["skipped"] = skipped
        stats["seconds"] = time.perf_counter() - file_start


def find_csv_files(folder):
    if not os.path.isdir(folder):
        print("Fout: CSV-map bestaat niet:")
        print(f"  {os.path.abspath(folder)}")
        sys.exit(1)

    try:
        entries = os.listdir(folder)
    except OSError as error:
        print(f"Kan map niet openen: {error}")
        sys.exit(1)

    files = [
        os.path.join(folder, name)
        for name in entries
        if name.lower().endswith(".csv")
        and os.path.isfile(os.path.join(folder, name))
    ]

    files.sort()
    return files


def print_summary(results, upload_seconds):
    total_read = sum(r["read"] for r in results)
    total_inserted = sum(r["inserted"] for r in results)
    total_skipped = sum(r["skipped"] for r in results)
    program_seconds = time.perf_counter() - PROGRAM_START

    name_width = min(max(len(r["file"]) for r in results), 40)
    name_width = max(name_width, len("Bestand"))

    header = (
        f"{'Bestand'.ljust(name_width)} | {'Gelezen':>10} | {'Toegevoegd':>10} | "
        f"{'Overgesl.':>9} | {'Duur':>8} | {'Regels/min':>10} | Status"
    )

    print()
    print("=" * len(header))
    print("OVERZICHT PER BESTAND")
    print("=" * len(header))
    print(header)
    print("-" * len(header))

    for r in results:
        name = r["file"]
        if len(name) > name_width:
            name = name[: name_width - 3] + "..."

        print(
            f"{name.ljust(name_width)} | {r['read']:>10,} | {r['inserted']:>10,} | "
            f"{r['skipped']:>9,} | {format_duration(r['seconds']):>8} | "
            f"{per_minute(r['inserted'], r['seconds']):>10,.0f} | {r['status']}"
        )

    print("-" * len(header))
    print()
    print("TOTALEN")
    print(f"  Bestanden verwerkt:       {len(results):,}")
    print(f"  Regels gelezen:           {total_read:,}")
    print(f"  Regels toegevoegd:        {total_inserted:,}")
    print(f"  Regels overgeslagen:      {total_skipped:,}")
    print(f"  Totale uploadduur:        {format_duration(upload_seconds)}")
    print(f"  Totale programmaduur:     {format_duration(program_seconds)}")
    print(
        f"  Gemiddeld gelezen:        "
        f"{per_minute(total_read, upload_seconds):,.0f} regels/min"
    )
    print(
        f"  Gemiddelde uploadsnelheid: "
        f"{per_minute(total_inserted, upload_seconds):,.0f} regels/min"
    )


def main():
    print()
    print("==============================================")
    print("       CSV -> MariaDB uploader")
    print("==============================================")
    print()

    vehicle_name = input("Voertuignaam: ").strip()

    if not vehicle_name:
        print("Fout: voertuignaam mag niet leeg zijn.")
        sys.exit(1)

    datetime_text = input("Startdatum/tijd (YYYYMMDDTHHMMSS): ").strip()

    try:
        start_datetime = parse_start_datetime(datetime_text)
    except ValueError as error:
        print(f"Fout: {error}")
        sys.exit(1)

    csv_files = find_csv_files(CSV_FOLDER)

    if not csv_files:
        print()
        print("Geen CSV-bestanden gevonden in:")
        print(f"  {os.path.abspath(CSV_FOLDER)}")
        sys.exit(0)

    print()
    print(f"{len(csv_files)} CSV-bestand(en) gevonden.")
    print(f"CSV-map: {os.path.abspath(CSV_FOLDER)}")
    print()

    print("Verbinden met database...")
    connection = connect_database()
    print("Databaseverbinding OK.")

    results = []
    upload_start = time.perf_counter()

    try:
        total_files = len(csv_files)

        for file_number, filepath in enumerate(csv_files, start=1):
            result = process_csv_file(
                connection,
                filepath,
                vehicle_name,
                start_datetime,
                file_number,
                total_files,
            )
            results.append(result)
    finally:
        connection.close()

    upload_seconds = time.perf_counter() - upload_start

    print_summary(results, upload_seconds)

    print()
    print("==============================================")
    print("KLAAR")
    print("==============================================")


if __name__ == "__main__":
    main()