# B2 Storage Manager

A desktop file manager for Backblaze B2. Browse buckets, upload and download
folders, and queue uploads from the Windows right-click menu.

Backblaze's own web console is fine for one file at a time. It is painful for
day-to-day work: no folder uploads and no right-click "send this to B2". This
replaces that round trip.

## What it does

- Browse buckets and folders, with a local SQLite cache so re-opening a large
  bucket does not re-list it from the API
- Upload whole folders, preserving structure, in parallel
- Large files are uploaded in parts, each sent with its own SHA-1 so Backblaze
  can verify it. Failed uploads are not resumed automatically.
- Download files and folders
- Queue uploads from the Windows shell context menu
- Native single-window GUI, no browser, no Electron

## Setup

On first run the app asks for a Backblaze **Key ID** and **Application Key**.
Create one in the Backblaze console under *Account › Application Keys*.

Credentials are never compiled into the binary. They are saved under
`~/.b2storage/` as `config.dat` and `auth_cache.dat`, encrypted with Windows DPAPI,
so they can only be decrypted by the Windows user that saved them. The app asks
once; later launches connect directly. To switch accounts, use *Use different
credentials* on the connection screen.

Earlier versions stored these in plaintext `.json` files. They are migrated to the
encrypted files on first launch and the plaintext copies are deleted.

DPAPI protects against another user, a copied disk or a backup that leaves the
machine. It does not protect against malware already running as you.

## Build from source

Requires a Rust toolchain (stable) and the MSVC target on Windows.

```
cargo build --release
```

The result is a single portable executable at
`target/release/b2storage.exe`. There is no installer and no runtime to
install — the C runtime is linked statically.

## Software rendering on machines with no GPU

The build embeds the [Mesa 3D](https://www.mesa3d.org/) software OpenGL
renderer and delay-loads `opengl32.dll`, so the GUI still works on a Windows
VPS or over RDP where no usable GPU driver exists. The MinGW Mesa build is used
deliberately: the MSVC build needs the Visual C++ redistributable, which a
freshly installed Windows Server does not have.

To re-vendor Mesa (requires 7-Zip on PATH):

```
python scripts/fetch-mesa.py
```

Each payload's length and SHA-256 are recorded in
`vendor/mesa/<arch>/manifest.txt`. `build.rs` decompresses every payload and fails
the build on a mismatch. At runtime the app checks the hash again before loading a
DLL, including one it unpacked on an earlier run, rather than trusting its size.

The manifest lives in the same repository as the payload, so this catches
corruption and accidental swaps, not a deliberately replaced pair. Provenance comes
from `scripts/fetch-mesa.py`, which downloads from the upstream release and writes
the manifest. See `vendor/mesa/NOTICE.md` for attribution.

## Tests and releases

```
cargo test --locked
```

CI (`.github/workflows/ci.yml`) runs the tests and a release build on every push.
Pushing a `v*` tag publishes a GitHub Release with the exe and a `SHA256SUMS.txt`.
The release binary is not code-signed.

## License

MIT — see [LICENSE](LICENSE). Vendored third-party binaries carry their own
licenses; see `vendor/mesa/NOTICE.md`.
