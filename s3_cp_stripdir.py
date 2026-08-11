 #!/usr/bin/env python3
import argparse
import math
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


COPYOBJECT_MAX = 5 * 1024**3  # 5 GiB
PART_SIZE = 256 * 1024**2     # 256 MiB


def parse_endpoint(endpoint: str) -> str:
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint
    return "http://" + endpoint


def make_s3_client(endpoint: str, access_key: str, secret_key: str, region: str, verify_tls: bool):
    endpoint_url = parse_endpoint(endpoint)
    cfg = Config(
        region_name=region,
        s3={"addressing_style": "path"},
        retries={"max_attempts": 10, "mode": "standard"},
    )
    session = boto3.session.Session()
    return session.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=cfg,
        verify=verify_tls,
    )


def ensure_bucket(s3, bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
        return
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchBucket", "NotFound"):
            pass
        else:
            raise
    s3.create_bucket(Bucket=bucket)


def list_objects(s3, bucket: str, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"], obj["Size"]


def head_object_metadata(s3, bucket: str, key: str) -> Tuple[Optional[str], Optional[str], Dict[str, str]]:
    r = s3.head_object(Bucket=bucket, Key=key)
    content_type = r.get("ContentType")
    cache_control = r.get("CacheControl")
    metadata = r.get("Metadata") or {}
    return content_type, cache_control, metadata


def simple_copy(s3, src_bucket: str, dst_bucket: str, src_key: str, dst_key: str, extra_args: dict):
    s3.copy_object(
        Bucket=dst_bucket,
        Key=dst_key,
        CopySource={"Bucket": src_bucket, "Key": src_key},
        MetadataDirective="REPLACE",
        **extra_args,
    )


def multipart_copy(s3, src_bucket: str, dst_bucket: str, src_key: str, dst_key: str, size: int, extra_args: dict):
    mpu = s3.create_multipart_upload(Bucket=dst_bucket, Key=dst_key, **extra_args)
    upload_id = mpu["UploadId"]

    try:
        parts = []
        part_count = math.ceil(size / PART_SIZE)

        for part_number in range(1, part_count + 1):
            start = (part_number - 1) * PART_SIZE
            end = min(start + PART_SIZE, size) - 1

            resp = s3.upload_part_copy(
                Bucket=dst_bucket,
                Key=dst_key,
                PartNumber=part_number,
                UploadId=upload_id,
                CopySource={"Bucket": src_bucket, "Key": src_key},
                CopySourceRange=f"bytes={start}-{end}",
            )
            parts.append({"ETag": resp["CopyPartResult"]["ETag"], "PartNumber": part_number})

        s3.complete_multipart_upload(
            Bucket=dst_bucket,
            Key=dst_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

    except Exception:
        try:
            s3.abort_multipart_upload(Bucket=dst_bucket, Key=dst_key, UploadId=upload_id)
        except Exception:
            pass
        raise


def resolve_dst_key(src_key: str, strip_prefix: str) -> str:
    """Strip leading prefix from key for destination. Trailing slash is added automatically."""
    if strip_prefix and src_key.startswith(strip_prefix):
        dst_key = src_key[len(strip_prefix):]
        # Защита от пустого ключа, если prefix == key (не должно быть, но на всякий случай)
        return dst_key if dst_key else src_key
    return src_key


def copy_one(s3, src_bucket: str, dst_bucket: str, src_key: str, dst_key: str, size: int):
    content_type, cache_control, metadata = head_object_metadata(s3, src_bucket, src_key)

    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type
    if cache_control:
        extra_args["CacheControl"] = cache_control
    if metadata:
        extra_args["Metadata"] = metadata

    if size <= COPYOBJECT_MAX:
        simple_copy(s3, src_bucket, dst_bucket, src_key, dst_key, extra_args)
    else:
        multipart_copy(s3, src_bucket, dst_bucket, src_key, dst_key, size, extra_args)


def main():
    ap = argparse.ArgumentParser(
        description="Server-side bucket clone in MinIO/S3 (no object data flows through the client)."
    )
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--access-key", required=True)
    ap.add_argument("--secret-key", required=True)
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--src-bucket", required=True)
    ap.add_argument("--dst-bucket", required=True)
    ap.add_argument(
        "--prefix", default="",
        help="Copy only keys with this prefix (e.g. 'dump/'). "
             "The prefix itself is stripped from destination keys by default.",
    )
    ap.add_argument(
        "--no-strip-prefix", action="store_true", default=False,
        help="Keep the prefix as-is in destination keys (do not strip it).",
    )
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--insecure", action="store_true", default=True)
    args = ap.parse_args()

    # Нормализуем prefix: если передан без trailing slash — добавляем,
    # чтобы не захватить ключи вида "dump_something"
    prefix = args.prefix
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    strip_prefix = "" if args.no_strip_prefix else prefix

    s3 = make_s3_client(
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        region=args.region,
        verify_tls=not args.insecure,
    )

    ensure_bucket(s3, args.dst_bucket)

    futures = {}
    total = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for src_key, size in list_objects(s3, args.src_bucket, prefix):
            dst_key = resolve_dst_key(src_key, strip_prefix)
            total += 1
            future = ex.submit(copy_one, s3, args.src_bucket, args.dst_bucket, src_key, dst_key, size)
            futures[future] = (src_key, dst_key)

    for f in as_completed(futures):
        src_key, dst_key = futures[f]
        try:
            f.result()
        except Exception as e:
            failed += 1
            print(f"[ERROR] {src_key} → {dst_key}: {e}", file=sys.stderr)

    print(f"Done. Total objects: {total}, failed: {failed}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()

# python3 s3_cp_stripdir.py --endpoint https://file.aiagents.inno.local:9000 --access-key minio --secret-key minio-password --src-bucket gk-t1-sfera-prod-ppwi-search-copy --dst-bucket gk-t1-sfera-prod-ppwi-load-test --prefix dump/ --insecure
# python3 s3_copy.py --endpoint localhost:9000 --access-key minioadmin --secret-key minioadmin --src-bucket gk-t1-sfera-prod-ppwi-search --dst-bucket gk-t1-sfera-prod-ppwi-search-copy --insecure --no-ssl-warn