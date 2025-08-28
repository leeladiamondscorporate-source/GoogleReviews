#!/usr/bin/env python3
"""
main.py

1. Reads a merchant CSV feed (URL or local file)
2. Generates a Google-Shopping product_reviews XML with realistic reviews
3. Uploads the XML to Google Cloud Storage under Googlefinal/
"""

import os
import argparse
import pandas as pd
from faker import Faker
import random
import xml.etree.ElementTree as ET
from dateutil import tz
from tqdm import tqdm
from google.cloud import storage

def parse_args():
    p = argparse.ArgumentParser(description="Generate & upload Leela Diamonds reviews XML")
    p.add_argument(
        "--csv-source",
        required=True,
        help="URL or local path to your combined_google_merchant_feed CSV"
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
        "--gcs-bucket",
        required=True,
        help="Name of your GCS bucket (e.g. sitemaps.leeladiamond.com)"
    )
    p.add_argument(
        "--gcs-dest",
        default="Googlefinal/leela_reviews.xml",
        help="Destination path in the bucket"
    )
    return p.parse_args()

def generate_reviews_xml(df: pd.DataFrame, output_path: str, n_per_product: int):
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

    # XML skeleton
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

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Products"):
        pid = row["id"]
        url = row["link"]
        name = row.get("title", pid)
        for _ in range(n_per_product):
            rev = ET.SubElement(reviews_el, "review")
            ET.SubElement(rev, "review_id").text = str(review_id)

            reviewer = ET.SubElement(rev, "reviewer")
            ET.SubElement(reviewer, "name").text = fake.name()

            ts = fake.date_time_between(start_date="-90d", end_date="now", tzinfo=utc)
            ET.SubElement(rev, "review_timestamp").text = ts.isoformat()

            ET.SubElement(rev, "title").text = random.choice(titles)
            ET.SubElement(
                rev, "content"
            ).text = random.choice(templates).format(name=name)

            ET.SubElement(rev, "review_url", {"type": "singleton"}).text = url

            ratings = ET.SubElement(rev, "ratings")
            ET.SubElement(ratings, "overall", {"min": "1", "max": "5"}).text = str(
                random.randint(4, 5)
            )

            products = ET.SubElement(rev, "products")
            product = ET.SubElement(products, "product")
            pids = ET.SubElement(product, "product_ids")

            gtins = ET.SubElement(pids, "gtins")
            ET.SubElement(gtins, "gtin").text = pid + "CA"
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
    print(f"Generated {review_id-1} reviews → {output_path}")

def upload_to_gcs(local_file: str, bucket_name: str, dest_path: str):
    client = storage.Client()  # uses ADC from GOOGLE_APPLICATION_CREDENTIALS
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_path)
    blob.upload_from_filename(local_file)
    print(f"Uploaded → gs://{bucket_name}/{dest_path}")

def main():
    args = parse_args()

    # 1. Load CSV (URL or local)
    if args.csv_source.startswith(("http://", "https://")):
        df = pd.read_csv(args.csv_source, dtype=str)
    else:
        df = pd.read_csv(args.csv_source, dtype=str)
    print(f"Loaded {len(df)} products from {args.csv_source}")

    # 2. Generate XML
    generate_reviews_xml(df, args.output, args.n_per_product)

    # 3. Upload to GCS
    upload_to_gcs(args.output, args.gcs_bucket, args.gcs_dest)

if __name__ == "__main__":
    main()
