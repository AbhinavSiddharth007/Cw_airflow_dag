#!/usr/bin/env python3
import argparse
import logging
import os
import random
import string
import time
from datetime import datetime, timezone, date
from decimal import Decimal, ROUND_HALF_UP

from faker import Faker
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------
# Configuration & Utilities
# ---------------------------

FUEL_TYPES = [
    "HyperMatter", "Antimatter", "Hydrogen", "Deuterium", "Tri-Tachyon",
    "QuantumFlux", "Plasma", "DarkIon", "Helium-3"
]

SERVICE_MENU = [
    "hull patch", "oxygen refill", "hyperdrive check", "radiation scrub",
    "life-support tune-up", "gyro recalibration", "sensor alignment",
    "cargo sealant", "RCS fuel", "shield recharge"
]

# Popular ships/franchises from games, movies, and books
FRANCHISE_SHIPS = {
    "Star Wars": ["Millennium Falcon", "X-Wing", "Slave I", "TIE Advanced", "Ghost"],
    "Mass Effect": ["SSV Normandy SR-1", "SSV Normandy SR-2", "Tempest"],
    "Halo": ["Pillar of Autumn", "In Amber Clad", "Spirit of Fire"],
    "Star Trek": ["USS Enterprise", "USS Defiant", "USS Voyager", "USS Discovery"],
    "Firefly": ["Serenity"],
    "The Expanse": ["Rocinante", "Canterbury", "Agatha King"],
    "Elite Dangerous": ["Cobra Mk III", "Asp Explorer", "Anaconda"],
    "No Man's Sky": ["Exotic S-Class", "Hauler C-Class", "Explorer A-Class"],
    "Dune": ["Heighliner", "Ornithopter"],
    "Battlestar Galactica": ["Galactica", "Pegasus", "Raptor"],
    "Star Citizen": ["Constellation Andromeda", "Cutlass Black", "Carrack"],
    "Alien": ["USCSS Nostromo", "USCSS Prometheus"]
}

SPECIES = [
    "Human", "Asari", "Turian", "Sangheili", "Vulcan", "Twi'lek",
    "Belter", "Kree", "Time Lord", "Zabrak", "Klingon", "Protoss"
]

fake = Faker()

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )

def ensure_data_dir(path: str = "data"):
    os.makedirs(path, exist_ok=True)
    logging.info(f'Ensured output directory exists: "{path}"')

def random_ship_and_franchise():
    franchise = random.choice(list(FRANCHISE_SHIPS.keys()))
    ship = random.choice(FRANCHISE_SHIPS[franchise])
    return ship, franchise

def random_station_id():
    # Keep it simple & realistic-ish
    return random.randint(1000, 9999)

def random_dock_struct():
    # Example struct with an int bay and string level
    return {"bay": random.randint(1, 128), "level": random.choice(list(string.ascii_uppercase[:8]))}

def random_services():
    k = random.randint(1, 4)
    return random.sample(SERVICE_MENU, k=k)

def money_decimal(minimum=5, maximum=500, quant="0.01"):
    value = Decimal(random.uniform(minimum, maximum)).quantize(Decimal(quant), rounding=ROUND_HALF_UP)
    return value

def random_uuid_like():
    # Not a strict UUID, but handy if you want no extra dependency.
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    rand = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
    return f"{ts}-{rand}"

def build_schema():
    """
    Mix of types including array and struct.
    """
    return pa.schema([
        ("transaction_id", pa.string()),
        ("station_id", pa.int32()),
        ("dock", pa.struct([("bay", pa.int16()), ("level", pa.string())])),
        ("ship_name", pa.string()),
        ("franchise", pa.string()),
        ("captain_name", pa.string()),
        ("species", pa.string()),
        ("fuel_type", pa.string()),
        ("fuel_units", pa.float32()),
        ("price_per_unit", pa.decimal128(8, 2)),  # up to 999,999.99
        ("total_cost", pa.decimal128(12, 2)),     # up to 9,999,999,999.99
        ("services", pa.list_(pa.string())),
        ("is_emergency", pa.bool_()),
        ("visited_at", pa.timestamp("ns", tz="UTC")),
        ("arrival_date", pa.date32()),
        ("coords_x", pa.float64()),
        ("coords_y", pa.float64()),
    ])

def make_record():
    ship_name, franchise = random_ship_and_franchise()
    captain = fake.name()
    sp = random.choice(SPECIES)
    fuel_type = random.choice(FUEL_TYPES)
    fuel_units = round(random.uniform(50, 5000), 2)
    ppu = money_decimal(10, 800)  # price per unit
    total = (Decimal(str(fuel_units)) * ppu).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    visited = datetime.now(timezone.utc)
    services = random_services()
    emergency = random.random() < 0.03  # ~3% emergencies
    coords_x = random.uniform(-10000.0, 10000.0)
    coords_y = random.uniform(-10000.0, 10000.0)

    return {
        "transaction_id": random_uuid_like(),
        "station_id": random_station_id(),
        "dock": random_dock_struct(),
        "ship_name": ship_name,
        "franchise": franchise,
        "captain_name": captain,
        "species": sp,
        "fuel_type": fuel_type,
        "fuel_units": float(fuel_units),
        "price_per_unit": ppu,
        "total_cost": total,
        "services": services,                 # <-- array/list<string>
        "is_emergency": emergency,
        "visited_at": visited,                # <-- timestamp with timezone
        "arrival_date": date(visited.year, visited.month, visited.day),  # <-- date
        "coords_x": coords_x,
        "coords_y": coords_y,
    }

def make_batch(n_rows: int):
    return [make_record() for _ in range(n_rows)]

def write_parquet(records, out_dir: str, schema: pa.Schema):
    table = pa.Table.from_pylist(records, schema=schema)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"fuel_export_{ts}.parquet"
    out_path = os.path.join(out_dir, filename)

    pq.write_table(table, out_path, compression="snappy")
    size_bytes = os.path.getsize(out_path)

    logging.info(
        f'Wrote file: {out_path} | rows={table.num_rows} | cols={table.num_columns} | size={size_bytes} bytes'
    )

def main():
    parser = argparse.ArgumentParser(
        description="Continuously generate Parquet files with synthetic interspace fuel station transactions."
    )
    parser.add_argument("--rows-per-file", type=int, default=300,
                        help="Number of rows per Parquet file (default: 300)")
    parser.add_argument("--period-seconds", type=int, default=60,
                        help="Seconds between files (default: 60)")
    parser.add_argument("--out-dir", type=str, default="data",
                        help='Output directory (default: "data")')
    args = parser.parse_args()

    setup_logging()
    ensure_data_dir(args.out_dir)
    schema = build_schema()

    logging.info("Starting continuous generation. Press Ctrl+C to stop.")
    logging.info(f"Rows per file: {args.rows_per_file} | Period: {args.period_seconds}s | Output: {args.out_dir}")

    try:
        while True:
            records = make_batch(args.rows_per_file)
            write_parquet(records, args.out_dir, schema)
            time.sleep(args.period_seconds)
    except KeyboardInterrupt:
        logging.info("Stopped by user. Goodbye!")

if __name__ == "__main__":
    main()
