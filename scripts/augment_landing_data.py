#!/usr/bin/env python3
"""Add products, customer feedback, and CDC tombstones to landing data.

The generator is deterministic and intentionally does not modify existing source
files. It updates landing/_manifest.json after all new files are written.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


SEED = 20260728
UTC = timezone.utc


PRODUCT_TAXONOMY = {
    "electronics": [
        "phones",
        "laptops",
        "audio",
        "cameras",
        "smart_home",
    ],
    "fashion": [
        "mens_clothing",
        "womens_clothing",
        "shoes",
        "bags",
        "accessories",
    ],
    "home": [
        "kitchen",
        "furniture",
        "bedding",
        "lighting",
        "decor",
    ],
    "beauty": [
        "skincare",
        "makeup",
        "haircare",
        "fragrance",
        "personal_care",
    ],
    "sports": [
        "running",
        "fitness",
        "cycling",
        "outdoor",
        "team_sports",
    ],
    "books": [
        "fiction",
        "business",
        "technology",
        "education",
        "children",
    ],
    "toys": [
        "building_sets",
        "action_figures",
        "board_games",
        "educational",
        "outdoor_toys",
    ],
    "grocery": [
        "beverages",
        "snacks",
        "pantry",
        "organic",
        "household",
    ],
    "automotive": [
        "car_care",
        "electronics",
        "interior",
        "exterior",
        "tools",
    ],
    "pet_supplies": [
        "dog",
        "cat",
        "fish",
        "bird",
        "small_pet",
    ],
}

BRANDS = [
    "Nova",
    "Aster",
    "Vertex",
    "Nimbus",
    "Orion",
    "Luma",
    "Kite",
    "Harbor",
    "Mosaic",
    "Pulse",
    "Atlas",
    "Cedar",
]

FEEDBACK_TEMPLATES = {
    ("delivery", "positive"): [
        "Delivery was earlier than expected and the package arrived safely.",
        "The courier was professional and delivery tracking was accurate.",
        "Fast delivery, careful packaging, and no damage at all.",
    ],
    ("delivery", "negative"): [
        "Delivery was late and the tracking status did not update.",
        "The package arrived damaged after a long delivery delay.",
        "The courier missed the promised delivery window twice.",
    ],
    ("delivery", "neutral"): [
        "Delivery arrived on the estimated date.",
        "The package was delivered with standard shipping.",
    ],
    ("product_quality", "positive"): [
        "The product feels durable and works exactly as described.",
        "Excellent build quality and better than I expected.",
        "The item matches the photos and performs very well.",
    ],
    ("product_quality", "negative"): [
        "The product stopped working after two days.",
        "The material feels cheap and the item does not match the description.",
        "The product arrived with a visible defect.",
    ],
    ("product_quality", "neutral"): [
        "The product is acceptable for the price.",
        "The item works, but there is nothing especially notable about it.",
    ],
    ("payment", "positive"): [
        "Payment was quick and the receipt arrived immediately.",
        "The checkout process was simple and secure.",
    ],
    ("payment", "negative"): [
        "My payment was charged twice and I need a refund.",
        "The checkout page failed several times before accepting payment.",
        "The discount code was valid but was not applied.",
    ],
    ("payment", "neutral"): [
        "Payment completed with my usual card.",
        "The order was paid without any additional issue.",
    ],
    ("support", "positive"): [
        "Customer support solved my issue in one conversation.",
        "The support agent was patient, clear, and helpful.",
    ],
    ("support", "negative"): [
        "Support did not answer my question and closed the ticket.",
        "I waited several days without receiving a useful response.",
        "The agent transferred me repeatedly without resolving the issue.",
    ],
    ("support", "neutral"): [
        "Support replied with the standard return instructions.",
        "The help desk acknowledged my request.",
    ],
    ("other", "positive"): [
        "Overall this was a smooth shopping experience.",
        "I am satisfied and would order from this store again.",
    ],
    ("other", "negative"): [
        "The overall shopping experience was confusing and frustrating.",
        "Several small issues made this order disappointing.",
    ],
    ("other", "neutral"): [
        "I have no strong opinion about this order.",
        "The experience was generally average.",
    ],
}


def iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def manifest_relative(path: Path) -> str:
    parts = path.resolve().parts
    landing_index = parts.index("landing")
    return Path(*parts[landing_index + 1 :]).as_posix()


def write_jsonl(path: Path, records: list[dict | str]) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            line = record if isinstance(record, str) else json.dumps(
                record, ensure_ascii=False, sort_keys=True
            )
            encoded = (line + "\n").encode("utf-8")
            digest.update(encoded)
            handle.write(line + "\n")
    return {
        "path": manifest_relative(path),
        "rows": len(records),
        "sha256": digest.hexdigest(),
        "format": "json",
    }


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return {
        "path": manifest_relative(path),
        "rows": len(rows),
        "sha256": digest,
        "format": "csv",
    }


def collect_order_ids(landing: Path, limit: int = 30_000) -> list[str]:
    order_ids: set[str] = set()
    for path in sorted((landing / "orders").rglob("*.json")):
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                order_id = value.get("order_id")
                if order_id:
                    order_ids.add(str(order_id))
                if len(order_ids) >= limit:
                    return sorted(order_ids)
    return sorted(order_ids)


def generate_products(landing: Path, rng: random.Random) -> tuple[list[dict], dict]:
    initial_rows: list[dict] = []
    category_items = list(PRODUCT_TAXONOMY.items())
    created_at = datetime(2026, 5, 1, tzinfo=UTC)
    for index in range(1000):
        category_index = index % len(category_items)
        category_name, subcategories = category_items[category_index]
        subcategory_name = subcategories[(index // len(category_items)) % len(subcategories)]
        base_cost = round(rng.uniform(3.5, 480.0), 2)
        initial_rows.append(
            {
                "sku": f"sku_{index:05d}",
                "product_name": (
                    f"{BRANDS[index % len(BRANDS)]} "
                    f"{subcategory_name.replace('_', ' ').title()} {index + 1}"
                ),
                "brand": BRANDS[index % len(BRANDS)],
                "subcategory_code": f"SC{category_index + 1:02d}{subcategories.index(subcategory_name) + 1:02d}",
                "subcategory_name": subcategory_name,
                "category_code": f"C{category_index + 1:02d}",
                "category_name": category_name,
                "unit_cost": f"{base_cost:.2f}",
                "updated_at": iso(created_at),
            }
        )

    product_fields = list(initial_rows[0])
    files = [
        write_csv(
            landing
            / "products/load_date=2026-05-01/hour=00/"
            "batch_id=products_20260501_00/products.csv",
            initial_rows,
            product_fields,
        )
    ]

    update_rows: list[dict] = []
    for row in rng.sample(initial_rows, 60):
        changed = dict(row)
        changed["unit_cost"] = f"{float(row['unit_cost']) * rng.uniform(0.92, 1.15):.2f}"
        changed["updated_at"] = iso(datetime(2026, 5, 8, 1, tzinfo=UTC))
        update_rows.append(changed)
    files.append(
        write_csv(
            landing
            / "products/load_date=2026-05-08/hour=01/"
            "batch_id=products_20260508_01/products.csv",
            update_rows,
            product_fields,
        )
    )

    summary = {
        "rows": len(initial_rows) + len(update_rows),
        "unique_skus": len(initial_rows),
        "update_rows": len(update_rows),
        "batches": len(files),
        "files": files,
    }
    return initial_rows, summary


def feedback_record(
    index: int,
    rng: random.Random,
    order_ids: list[str],
    base_time: datetime,
) -> tuple[dict, dict]:
    category = rng.choice(["delivery", "product_quality", "payment", "support", "other"])
    sentiment = rng.choices(
        ["positive", "negative", "neutral"], weights=[0.48, 0.37, 0.15], k=1
    )[0]
    text = rng.choice(FEEDBACK_TEMPLATES[(category, sentiment)])
    user_id = f"user_{rng.randrange(0, 20_000):06d}"
    created_at = base_time + timedelta(minutes=rng.randrange(0, 7 * 24 * 60))
    if index % 97 == 0:
        text += f" Contact me at {user_id}@example.test."
    record = {
        "feedback_id": f"feedback_{index:07d}",
        "user_id": user_id,
        "order_id": rng.choice(order_ids) if order_ids and rng.random() < 0.88 else None,
        "feedback_text": text,
        "created_at": iso(created_at),
        "updated_at": iso(created_at),
        "language": "en",
    }
    label = {
        "feedback_id": record["feedback_id"],
        "expected_sentiment": sentiment,
        "expected_category": category,
    }
    return record, label


def generate_feedback(
    root: Path, landing: Path, rng: random.Random, order_ids: list[str]
) -> dict:
    base_time = datetime(2026, 5, 1, tzinfo=UTC)
    valid_records: list[dict] = []
    labels: list[dict] = []
    for index in range(4800):
        record, label = feedback_record(index, rng, order_ids, base_time)
        valid_records.append(record)
        labels.append(label)

    by_batch: dict[tuple[str, str], list[dict | str]] = defaultdict(list)
    for record in valid_records:
        dt = datetime.fromisoformat(record["created_at"].replace("Z", "+00:00"))
        batch_hour = (dt.hour // 8) * 8
        date_text = dt.strftime("%Y-%m-%d")
        hour_text = f"{batch_hour:02d}"
        by_batch[(date_text, hour_text)].append(record)

    update_records: list[dict] = []
    for original in rng.sample(valid_records, 150):
        updated = dict(original)
        updated["feedback_text"] += " Update: the issue has now been reviewed."
        updated["updated_at"] = iso(datetime(2026, 5, 9, 9, tzinfo=UTC))
        update_records.append(updated)
    by_batch[("2026-05-09", "09")].extend(update_records)

    invalid_records: list[dict | str] = []
    for index in range(20):
        record, _ = feedback_record(6000 + index, rng, order_ids, base_time)
        record["feedback_id"] = None
        invalid_records.append(record)
    for index in range(15):
        record, _ = feedback_record(6100 + index, rng, order_ids, base_time)
        record["feedback_text"] = ""
        invalid_records.append(record)
    for index in range(15):
        invalid_records.append(
            '{"feedback_id":"broken_%03d","feedback_text":"unterminated"' % index
        )
    by_batch[("2026-05-10", "10")].extend(invalid_records)

    files: list[dict] = []
    for (date_text, hour_text), records in sorted(by_batch.items()):
        files.append(
            write_jsonl(
                landing
                / (
                    f"customer_feedback/load_date={date_text}/hour={hour_text}/"
                    f"batch_id=feedback_{date_text.replace('-', '')}_{hour_text}/"
                    "feedback.json"
                ),
                records,
            )
        )

    labels_path = root / "data/expected_results/customer_feedback_gold_labels.csv"
    labels_path.parent.mkdir(parents=True, exist_ok=True)
    with labels_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["feedback_id", "expected_sentiment", "expected_category"],
        )
        writer.writeheader()
        writer.writerows(labels)

    return {
        "rows": sum(item["rows"] for item in files),
        "base_valid_records": len(valid_records),
        "update_records": len(update_records),
        "missing_key": 20,
        "empty_feedback_text": 15,
        "parse_error": 15,
        "contains_synthetic_pii": sum(
            "@example.test" in record["feedback_text"] for record in valid_records
        ),
        "batches": len(files),
        "gold_label_rows": len(labels),
        "gold_labels_path": labels_path.relative_to(root).as_posix(),
        "files": files,
    }


def generate_cdc_deletes(
    landing: Path,
    rng: random.Random,
    order_ids: list[str],
) -> dict:
    records: list[dict] = []
    chosen_orders = rng.sample(order_ids, min(200, len(order_ids)))
    for index, order_id in enumerate(chosen_orders):
        changed_at = datetime(2026, 5, 10, 8, tzinfo=UTC) + timedelta(
            minutes=index * 2
        )
        records.append(
            {
                "entity_type": "order",
                "business_key": order_id,
                "operation": "D",
                "changed_at": iso(changed_at),
                "reason": rng.choice(
                    ["privacy_request", "source_correction", "test_tombstone"]
                ),
                "source_lsn": f"order-delete-{index + 1:08d}",
            }
        )
    for index in range(100):
        changed_at = datetime(2026, 5, 11, 11, tzinfo=UTC) + timedelta(
            minutes=index * 3
        )
        records.append(
            {
                "entity_type": "customer",
                "business_key": f"user_{rng.randrange(0, 20_000):06d}",
                "operation": "D",
                "changed_at": iso(changed_at),
                "reason": "privacy_request",
                "source_lsn": f"customer-delete-{index + 1:08d}",
            }
        )

    by_batch: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for record in records:
        dt = datetime.fromisoformat(record["changed_at"].replace("Z", "+00:00"))
        by_batch[(dt.strftime("%Y-%m-%d"), f"{dt.hour:02d}")].append(record)

    files = []
    for (date_text, hour_text), batch_records in sorted(by_batch.items()):
        files.append(
            write_jsonl(
                landing
                / (
                    f"cdc_deletes/load_date={date_text}/hour={hour_text}/"
                    f"batch_id=deletes_{date_text.replace('-', '')}_{hour_text}/"
                    "deletes.json"
                ),
                batch_records,
            )
        )
    return {
        "rows": len(records),
        "order_deletes": len(chosen_orders),
        "customer_deletes": 100,
        "batches": len(files),
        "files": files,
    }


def update_manifest(
    landing: Path,
    product_summary: dict,
    feedback_summary: dict,
    delete_summary: dict,
) -> None:
    manifest_path = landing / "_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["augmented_at"] = iso(datetime.now(UTC))
    manifest["augmentation"] = {
        "seed": SEED,
        "generator": "scripts/augment_landing_data.py",
        "preserves_existing_sources": True,
    }
    manifest["sources"]["products"] = product_summary
    manifest["sources"]["customer_feedback"] = feedback_summary
    manifest["sources"]["cdc_deletes"] = delete_summary
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def repair_manifest_paths(landing: Path) -> None:
    manifest_path = landing / "_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for source in ("products", "customer_feedback", "cdc_deletes"):
        for item in manifest["sources"].get(source, {}).get("files", []):
            path = Path(item["path"])
            if path.is_absolute():
                item["path"] = manifest_relative(path)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--repair-manifest-paths", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    landing = root / "landing"
    if args.repair_manifest_paths:
        repair_manifest_paths(landing)
        print("Repaired augmentation paths in landing/_manifest.json")
        return
    targets = [
        landing / "products",
        landing / "customer_feedback",
        landing / "cdc_deletes",
        root / "data/expected_results/customer_feedback_gold_labels.csv",
    ]
    existing = [path for path in targets if path.exists()]
    if existing:
        raise SystemExit(
            "Refusing to overwrite existing augmentation targets: "
            + ", ".join(str(path) for path in existing)
        )

    rng = random.Random(SEED)
    order_ids = collect_order_ids(landing)
    _, product_summary = generate_products(landing, rng)
    feedback_summary = generate_feedback(root, landing, rng, order_ids)
    delete_summary = generate_cdc_deletes(landing, rng, order_ids)
    update_manifest(landing, product_summary, feedback_summary, delete_summary)

    print(
        json.dumps(
            {
                "products": product_summary["rows"],
                "feedback": feedback_summary["rows"],
                "cdc_deletes": delete_summary["rows"],
                "order_ids_sampled_from_existing_data": len(order_ids),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
