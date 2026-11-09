#!/usr/bin/env python3
"""Vendor the Mesa3D software OpenGL renderer into vendor/mesa/.

The app embeds these payloads in the exe (see build.rs) so a machine with no
GPU driver, such as a Windows VPS or an RDP session, can still render the GUI
through Mesa's llvmpipe driver.

Only two files per architecture are needed for desktop OpenGL:
  opengl32.dll        the ICD entry points
  libgallium_wgl.dll  the llvmpipe driver itself

The MinGW build is used on purpose: unlike the MSVC build it does not need the
Visual C++ redistributable, which a freshly installed Windows Server lacks.

Usage:  python scripts/fetch-mesa.py [--version 26.2.0] [--keep-download]
Requires 7-Zip (7z) on PATH or installed in Program Files.
"""

import argparse
import hashlib
import io
import lzma
import os
import shutil
import subprocess
import sys
import urllib.request

REPO = "pal1000/mesa-dist-win"
FILES = ["opengl32.dll", "libgallium_wgl.dll"]
ARCHES = ["x64", "x86"]

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VENDOR = os.path.join(ROOT, "vendor", "mesa")


def find_7z():
    found = shutil.which("7z") or shutil.which("7za")
    if found:
        return found
    for candidate in (
        r"C:\Program Files\7-Zip\7z.exe",
        r"C:\Program Files (x86)\7-Zip\7z.exe",
    ):
        if os.path.isfile(candidate):
            return candidate
    sys.exit("7-Zip not found. Install it from https://www.7-zip.org/ and retry.")


def latest_version():
    url = "https://api.github.com/repos/%s/releases/latest" % REPO
    with urllib.request.urlopen(url, timeout=60) as r:
        import json

        return json.load(r)["tag_name"]


def download(version, dest):
    name = "mesa3d-%s-release-mingw.7z" % version
    url = "https://github.com/%s/releases/download/%s/%s" % (REPO, version, name)
    print("downloading %s" % url)
    with urllib.request.urlopen(url, timeout=1800) as r, io.open(dest, "wb") as f:
        shutil.copyfileobj(r, f)
    print("saved %s (%.1f MB)" % (dest, os.path.getsize(dest) / 1048576.0))


def extract(seven_zip, archive, workdir):
    wanted = ["%s/%s" % (a, f) for a in ARCHES for f in FILES]
    subprocess.check_call(
        [seven_zip, "x", "-y", "-o" + workdir, archive] + wanted,
        stdout=subprocess.DEVNULL,
    )


def compress(version, workdir):
    filters = [{"id": lzma.FILTER_LZMA2, "preset": 9 | lzma.PRESET_EXTREME}]
    for arch in ARCHES:
        src_dir = os.path.join(workdir, arch)
        if not os.path.isdir(src_dir):
            print("skipping %s, not in archive" % arch)
            continue
        dst_dir = os.path.join(VENDOR, arch)
        os.makedirs(dst_dir, exist_ok=True)
        manifest = ["version %s" % version, "arch %s" % arch]
        for name in FILES:
            raw = io.open(os.path.join(src_dir, name), "rb").read()
            packed = lzma.compress(raw, format=lzma.FORMAT_XZ, filters=filters)
            io.open(os.path.join(dst_dir, name + ".xz"), "wb").write(packed)
            manifest.append(
                "%s %d %s" % (name, len(raw), hashlib.sha256(raw).hexdigest())
            )
            print(
                "%-4s %-22s %8.2f MB -> %6.2f MB"
                % (arch, name, len(raw) / 1048576.0, len(packed) / 1048576.0)
            )
        io.open(os.path.join(dst_dir, "manifest.txt"), "w", newline="\n").write(
            "\n".join(manifest) + "\n"
        )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", help="Mesa release tag, defaults to the latest")
    ap.add_argument(
        "--keep-download", action="store_true", help="keep the downloaded .7z"
    )
    args = ap.parse_args()

    seven_zip = find_7z()
    version = args.version or latest_version()
    print("Mesa version: %s" % version)

    workdir = os.path.join(ROOT, "target", "mesa-fetch")
    os.makedirs(workdir, exist_ok=True)
    archive = os.path.join(workdir, "mesa-%s.7z" % version)

    if not os.path.isfile(archive):
        download(version, archive)
    else:
        print("using cached %s" % archive)

    extract(seven_zip, archive, workdir)
    compress(version, workdir)

    if not args.keep_download:
        os.remove(archive)

    print("vendored into %s" % VENDOR)


if __name__ == "__main__":
    main()
