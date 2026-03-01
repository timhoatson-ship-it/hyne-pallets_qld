#!/usr/bin/env python3
"""Download or assemble index.html at startup if missing/truncated."""
import base64
import gzip
import glob
import os
import sys
import urllib.request
import hashlib

EXPECTED_MD5 = "9f5d46243765de82f19cceba120a006f"
EXPECTED_SIZE = 282245
DOWNLOAD_URL = "https://sites.pplx.app/sites/proxy/eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwcmVmaXgiOiJ3ZWIvZGlyZWN0LWZpbGVzL2NvbXB1dGVyLzEwNDk1ZjE1LThmNmEtNDcwNy04OWQzLTVjYjA2YjdiMmU4OC9oeW5lLWh0bWwtaG9zdC8iLCJzaWQiOiIxMDQ5NWYxNS04ZjZhLTQ3MDctODlkMy01Y2IwNmI3YjJlODgiLCJleHAiOjE3NzIzOTA5NzZ9.O5s3VCN9xvvrnmbVgZzOd9rvgddMgEggKFD6VzgfbB4/web/direct-files/computer/10495f15-8f6a-4707-89d3-5cb06b7b2e88/hyne-html-host/index.html"

index_path = os.path.join("static", "index.html")


def check_file():
    """Check if index.html exists and is valid."""
    if not os.path.exists(index_path):
        return False
    size = os.path.getsize(index_path)
    if size < 100000:
        return False
    with open(index_path, "rb") as f:
        md5 = hashlib.md5(f.read()).hexdigest()
    if md5 == EXPECTED_MD5:
        print(f"index.html OK ({size} bytes, md5={md5})")
        return True
    print(f"index.html exists but md5 mismatch ({md5} != {EXPECTED_MD5})")
    return False


def try_download():
    """Download index.html from backup URL."""
    try:
        print(f"Downloading index.html from backup URL...")
        urllib.request.urlretrieve(DOWNLOAD_URL, index_path)
        size = os.path.getsize(index_path)
        with open(index_path, "rb") as f:
            md5 = hashlib.md5(f.read()).hexdigest()
        print(f"Downloaded {size} bytes, md5={md5}")
        return md5 == EXPECTED_MD5
    except Exception as e:
        print(f"Download failed: {e}")
        return False


def try_gz_b64_parts():
    """Try combining gzip+base64 encoded parts."""
    parts = sorted(glob.glob(os.path.join("static", "index.gz.b64.part*")))
    if not parts:
        return False
    try:
        b64_data = ""
        for p in sorted(parts):
            with open(p, "r") as f:
                b64_data += f.read().strip()
            print(f"  Read {p} ({os.path.getsize(p)} bytes)")
        compressed = base64.b64decode(b64_data)
        html_content = gzip.decompress(compressed)
        with open(index_path, "wb") as f:
            f.write(html_content)
        print(f"Combined {len(parts)} gz+b64 parts ({len(html_content)} bytes)")
        return True
    except Exception as e:
        print(f"gz+b64 assembly failed: {e}")
        return False


if __name__ == "__main__":
    if check_file():
        sys.exit(0)

    # Strategy 1: Download from backup URL
    if try_download() and check_file():
        print("SUCCESS: Downloaded from backup URL")
        sys.exit(0)

    # Strategy 2: Assemble from gz+b64 parts
    if try_gz_b64_parts() and check_file():
        print("SUCCESS: Assembled from gz+b64 parts")
        sys.exit(0)

    # Check if we at least have something
    if os.path.exists(index_path) and os.path.getsize(index_path) > 100000:
        print(f"WARNING: index.html exists ({os.path.getsize(index_path)} bytes) but md5 doesn't match. Using anyway.")
        sys.exit(0)

    print("ERROR: Could not obtain valid index.html")
    sys.exit(1)
