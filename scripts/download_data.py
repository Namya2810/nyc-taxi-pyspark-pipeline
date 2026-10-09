"""Download the NYC TLC yellow-taxi trips for one month plus the taxi-zone lookup table."""
import sys
import urllib.request
from pathlib import Path

BASE = "https://d37ci6vzurychx.cloudfront.net"
RAW = Path(__file__).resolve().parents[1] / "data" / "raw"


def fetch(url: str, dest: Path) -> None:
    if dest.exists():
        print(f"already present: {dest.name}")
        return
    print(f"downloading {url}")
    urllib.request.urlretrieve(url, dest)
    print(f"saved {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")


def main(month: str = "2024-01") -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    fetch(f"{BASE}/trip-data/yellow_tripdata_{month}.parquet", RAW / f"yellow_tripdata_{month}.parquet")
    fetch(f"{BASE}/misc/taxi_zone_lookup.csv", RAW / "taxi_zone_lookup.csv")


if __name__ == "__main__":
    main(*sys.argv[1:2])
