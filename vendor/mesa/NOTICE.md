# Vendored third-party binaries

This folder contains prebuilt Mesa 3D binaries that `build.rs` embeds into the
executable, so the GUI renders on machines with no usable GPU driver (a Windows
VPS or an RDP session) through Mesa's llvmpipe software renderer.

## Mesa 3D 26.2.0

- Files: `opengl32.dll`, `libgallium_wgl.dll` (per architecture, xz-compressed)
- Upstream project: https://www.mesa3d.org/
- Windows build used: https://github.com/pal1000/mesa-dist-win (MinGW variant,
  chosen because it does not require the Visual C++ redistributable)
- Re-vendor with: `python scripts/fetch-mesa.py`
- SHA-256 checksums for each file are recorded in the per-architecture
  `manifest.txt`. `build.rs` verifies them at build time and the app verifies
  them again before loading the DLL.

### License

The core Mesa library is distributed under the MIT license. Mesa is an
aggregate of components and not every component is MIT; some carry their own
copyrights and licenses, such as the SGI Free Software License B. The
authoritative, per-component license text ships with the Mesa source
distribution as `docs/license.rst`.

No Mesa source has been modified. The binaries are redistributed unchanged from
the upstream Windows build linked above.
