# B2 Storage Manager

A desktop file manager for Backblaze B2. Browse buckets, upload and download
folders, and queue uploads from the Windows right-click menu.

Backblaze's own web console is fine for one file at a time. It is painful for
day-to-day work: no folder uploads, no resumable transfers, no right-click
"send this to B2". This replaces that round trip.

## What it does

- Browse buckets and folders, with a local SQLite cache so re-opening a large
  bucket does not re-list it from the API
- Upload whole folders, preserving structure, in parallel
- Chunked uploads with SHA-1 verification per part, so large files survive a
  dropped connection
- Download files and folders
- Queue uploads from the Windows shell context menu
- Native single-window GUI, no browser, no Electron

## Setup

On first run the app asks for a Backblaze **Key ID** and **Application Key**.
Create one in the Backblaze console under *Account › Application Keys*.

Credentials are written to `~/.b2storage/config.json` on your own machine and
are never compiled into the binary. The app asks once; later launches connect
directly. To switch accounts, use *Use different credentials* on the connection
screen.

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

Checksums for each payload are recorded in `vendor/mesa/<arch>/manifest.txt`
and verified at build time. See `vendor/mesa/NOTICE.md` for attribution.

## License

MIT — see [LICENSE](LICENSE). Vendored third-party binaries carry their own
licenses; see `vendor/mesa/NOTICE.md`.
