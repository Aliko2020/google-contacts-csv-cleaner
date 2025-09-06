import boto3
import csv
import io
import re

s3 = boto3.client("s3")

def clean_name(name: str) -> str:
    """Remove emojis/special chars and title-case the name."""
    # Keep only letters, spaces, hyphens, and apostrophes
    cleaned = re.sub(r"[^A-Za-z\s'-]", "", name)
    return cleaned.strip().title()

def lambda_handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(content))
    cleaned_rows = []
    seen_phones = set()

    for row in reader:
        phone = row.get("Phone 1 - Value", "").strip()
        first = clean_name(row.get("First Name", ""))
        last = clean_name(row.get("Last Name", ""))

        if not phone or not first or not last:
            continue

        if phone in seen_phones:
            continue
        seen_phones.add(phone)

        cleaned_rows.append({
            "First Name": first,
            "Last Name": last,
            "Phone 1 - Value": phone
        })

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["First Name", "Last Name", "Phone 1 - Value"])
    writer.writeheader()
    writer.writerows(cleaned_rows)

    cleaned_key = f"cleaned/{key}"
    s3.put_object(Bucket=bucket, Key=cleaned_key, Body=output.getvalue())

    return {
        "statusCode": 200,
        "body": f"File cleaned and saved to {cleaned_key}"
    }
