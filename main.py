#!/usr/bin/env python3
"""
main.py

1) Reads a merchant CSV feed (URL, local file, or r2:// path)
2) Generates a Google Shopping "product_reviews" XML (fake but realistic reviews)
3) Uploads the XML to Cloudflare R2 (e.g., Googlefinal/leela_reviews.xml)
"""

import os
import argparse
import random
import xml.etree.ElementTree as ET
from io import BytesIO

import pandas as pd
from faker import Faker
from dateutil import tz
from tqdm import tqdm
import boto3


def parse_args():
    p = argparse.ArgumentParser(description="Generate & upload Leela Diamonds reviews XML")
    p.add_argument(
        "--csv-source",
        required=False,
        help="URL, local path, or r2://bucket/path to combined_google_merchant_feed.csv"
    )
    p.add_argument(
        "--output",
        default="leela_reviews.xml",
        help="Local filename for the generated XML"
    )
    p.add_argument(
        "--n-per-product",
        type=int,
        default=2,
        help="Number of reviews to generate per product"
    )
    p.add_argument(
        "--r2-bucket",
        required=True,
        help="Name of your R2 bucket (e.g. leela-feeds)"
    )
    p.add_argument(
        "--r2-dest",
        default="Googlefinal/leela_reviews.xml",
        help="Destination object path in the bucket"
    )
    return p.parse_args()


def load_csv_anywhere(src: str) -> pd.DataFrame:
    """Load CSV from http(s), r2://bucket/key, or local path."""
    src = (src or "").strip()
    if not src:
        raise ValueError("csv-source is empty. Provide a URL, local path, or r2:// path.")

    if src.startswith("r2://"):
        without_scheme = src[5:]
        bucket_name, _, object_key = without_scheme.partition("/")
        if not bucket_name or not object_key:
            raise ValueError(f"Invalid R2 path: {src}")
        response = r2_client().get_object(Bucket=bucket_name, Key=object_key)
        return pd.read_csv(BytesIO(response["Body"].read()), dtype=str)

    # http(s):// or local file
    if src.startswith(("http://", "https://")):
        return pd.read_csv(src, dtype=str)

    if not os.path.exists(src):
        raise FileNotFoundError(f"Local CSV not found: {src}")

    return pd.read_csv(src, dtype=str)


def generate_reviews_xml(df: pd.DataFrame, output_path: str, n_per_product: int):
    """Create product_reviews XML per Google's 2.3 schema."""
    fake = Faker()
    titles = [
        "Absolutely Stunning",
        "Perfect in Every Way",
        "Exceeded My Expectations",
        "Brilliant Sparkle",
        "Impeccable Quality",
    ]
    templates = [
        "I’m so impressed with my diamond! The cut is flawless and shipping was super fast.",
        "What a beautiful diamond—its brilliance really caught everyone’s eye at my event.",
        "Great experience from start to finish. The stone arrived exactly as described.",
        "Fantastic service and the stone looks even better in person. Highly recommend!",
        "Very happy with my purchase—exceptional quality and clear, bright sparkle.",
    ]

    # XML skeleton with schema reference
    ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")
    feed = ET.Element(
        "feed",
        {
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xsi:noNamespaceSchemaLocation":
                "http://www.google.com/shopping/reviews/schema/product/2.3/product_reviews.xsd"
        },
    )
    ET.SubElement(feed, "version").text = "2.3"
    pub = ET.SubElement(feed, "publisher")
    ET.SubElement(pub, "name").text = "Leela Diamonds Reviews"
    ET.SubElement(pub, "favicon").text = "https://leeladiamond.com/favicon.png"
    reviews_el = ET.SubElement(feed, "reviews")

    review_id = 1
    utc = tz.gettz("UTC")

    # Defensive: ensure required columns exist
    required_cols = {"id", "link", "title"}
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"CSV missing required columns: {missing}. Found: {list(df.columns)}")

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Products"):
        pid = str(row["id"])
        url = str(row["link"])
        name = str(row.get("title", pid))

        for _ in range(max(0, int(n_per_product))):
            rev = ET.SubElement(reviews_el, "review")
            ET.SubElement(rev, "review_id").text = str(review_id)

            reviewer = ET.SubElement(rev, "reviewer")
            ET.SubElement(reviewer, "name").text = fake.name()

            ts = fake.date_time_between(start_date="-90d", end_date="now", tzinfo=utc)
            ET.SubElement(rev, "review_timestamp").text = ts.isoformat()

            ET.SubElement(rev, "title").text = random.choice(titles)
            ET.SubElement(rev, "content").text = random.choice(templates)

            ET.SubElement(rev, "review_url", {"type": "singleton"}).text = url

            ratings = ET.SubElement(rev, "ratings")
            ET.SubElement(ratings, "overall", {"min": "1", "max": "5"}).text = str(random.randint(4, 5))

            products = ET.SubElement(rev, "products")
            product = ET.SubElement(products, "product")
            pids = ET.SubElement(product, "product_ids")

            # No GTINs – we supply a pseudo-gtin for structure and real MPN for matching
            gtins = ET.SubElement(pids, "gtins")
            ET.SubElement(gtins, "gtin").text = f"{pid}CA"

            mpns = ET.SubElement(pids, "mpns")
            ET.SubElement(mpns, "mpn").text = pid

            brands = ET.SubElement(pids, "brands")
            ET.SubElement(brands, "brand").text = "Leela Diamonds"

            ET.SubElement(product, "product_name").text = name
            ET.SubElement(product, "product_url").text = url

            review_id += 1

    # write file
    tree = ET.ElementTree(feed)
    tree.write(output_path, encoding="utf-8", xml_declaration=True)
    print(f"Generated {review_id - 1} reviews → {output_path}")


def r2_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )


def upload_to_r2(local_file: str, bucket_name: str, dest_path: str):
    r2_client().upload_file(
        local_file,
        bucket_name,
        dest_path,
        ExtraArgs={
            "ContentType": "application/xml",
            "CacheControl": "public, max-age=86400",
        },
    )
    print(f"Uploaded -> r2://{bucket_name}/{dest_path}")


def main():
    args = parse_args()

    # Allow env fallback if someone forgot the arg
    csv_source = args.csv_source or os.environ.get("CSV_URL", "").strip()
    if not csv_source:
        raise SystemExit("ERROR: --csv-source not provided and CSV_URL env is empty.")

    # 1) Load CSV
    df = load_csv_anywhere(csv_source)
    print(f"Loaded {len(df)} products from {csv_source}")

    # 2) Generate XML
    generate_reviews_xml(df, args.output, args.n_per_product)

    # 3) Upload to R2
    upload_to_r2(args.output, args.r2_bucket, args.r2_dest)


if __name__ == "__main__":
    main()
