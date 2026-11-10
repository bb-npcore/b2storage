use std::io::Write;
use std::path::{Path, PathBuf};

fn main() {
    // Delay-load opengl32.dll so it's NOT loaded at process startup.
    // This lets our Mesa setup code run first, and when glutin later
    // calls WGL functions, the delay-load resolver picks up Mesa's
    // opengl32.dll from the application directory instead of System32.
    println!("cargo:rustc-link-arg=/DELAYLOAD:opengl32.dll");
    println!("cargo:rustc-link-lib=delayimp");

    embed_mesa();
}

/// Pick the vendored Mesa payload that matches the target architecture and
/// generate a Rust source file that embeds it in the binary. When no payload
/// is vendored the generated file declares an empty set and the app falls back
/// to looking for an `opengl32.dll` next to the exe at runtime.
fn embed_mesa() {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();

    // Mesa ships its binaries in x64 / x86 folders, Rust names the same
    // architectures x86_64 / x86.
    let arch = match target_arch.as_str() {
        "x86_64" => Some("x64"),
        "x86" => Some("x86"),
        _ => None,
    };

    let vendor = arch.map(|a| manifest_dir.join("vendor").join("mesa").join(a));
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("vendor").join("mesa").display()
    );

    let generated = out_dir.join("mesa_payload.rs");
    let mut out = String::new();

    match vendor.as_deref().and_then(read_manifest) {
        Some(payload) => {
            println!(
                "cargo:rerun-if-changed={}",
                payload.dir.join("manifest.txt").display()
            );
            out.push_str(&format!(
                "pub const MESA_VERSION: &str = {:?};\npub const MESA_ARCH: &str = {:?};\n",
                payload.version, payload.arch
            ));
            out.push_str("pub static MESA_FILES: &[(&str, &[u8], u64, &str)] = &[\n");
            for entry in &payload.files {
                let packed = payload.dir.join(format!("{}.xz", entry.name));
                if !packed.is_file() {
                    panic!("vendored Mesa payload {} is missing", packed.display());
                }
                println!("cargo:rerun-if-changed={}", packed.display());
                // Fails the build on a corrupted or swapped payload, so a bad DLL can
                // never be embedded into a release binary.
                verify_payload(&packed, entry);
                out.push_str(&format!(
                    "    ({:?}, include_bytes!({:?}), {}, {:?}),\n",
                    entry.name, packed, entry.raw_len, entry.sha256
                ));
            }
            out.push_str("];\n");
        }
        None => {
            if let Some(dir) = vendor.as_deref() {
                println!(
                    "cargo:warning=No Mesa payload at {}. The build will work but software \
                     rendering will need an opengl32.dll next to the exe. Run \
                     scripts/fetch-mesa.py to vendor one.",
                    dir.display()
                );
            } else {
                println!(
                    "cargo:warning=No Mesa payload for target architecture {}",
                    target_arch
                );
            }
            out.push_str("pub const MESA_VERSION: &str = \"none\";\n");
            out.push_str("pub const MESA_ARCH: &str = \"none\";\n");
            out.push_str("pub static MESA_FILES: &[(&str, &[u8], u64, &str)] = &[];\n");
        }
    }

    let mut f = std::fs::File::create(&generated).expect("cannot create mesa_payload.rs");
    f.write_all(out.as_bytes()).expect("cannot write mesa_payload.rs");
}

struct Entry {
    name: String,
    raw_len: u64,
    sha256: String,
}

/// Decompresses `packed` and checks it against the length and SHA-256 recorded in
/// manifest.txt. The manifest hashes the decompressed DLL, which is what gets loaded.
fn verify_payload(packed: &Path, entry: &Entry) {
    use sha2::{Digest, Sha256};

    let compressed = std::fs::read(packed)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", packed.display()));
    let mut raw = Vec::with_capacity(entry.raw_len as usize);
    lzma_rs::xz_decompress(&mut std::io::Cursor::new(compressed), &mut raw)
        .unwrap_or_else(|e| panic!("{} is not a valid xz file: {e}", packed.display()));

    if raw.len() as u64 != entry.raw_len {
        panic!(
            "{}: decompressed to {} bytes, manifest says {}",
            entry.name,
            raw.len(),
            entry.raw_len
        );
    }

    let actual: String = Sha256::digest(&raw).iter().map(|b| format!("{b:02x}")).collect();
    if !actual.eq_ignore_ascii_case(&entry.sha256) {
        panic!(
            "{}: SHA-256 mismatch\n  manifest: {}\n  payload:  {}\nRe-vendor with scripts/fetch-mesa.py, \
             or investigate why the payload changed.",
            entry.name, entry.sha256, actual
        );
    }
}

struct Payload {
    dir: PathBuf,
    version: String,
    arch: String,
    files: Vec<Entry>,
}

/// manifest.txt format:
///   version 26.2.0
///   arch x64
///   <file name> <uncompressed byte length> <sha256 of the uncompressed file>
fn read_manifest(dir: &Path) -> Option<Payload> {
    let text = std::fs::read_to_string(dir.join("manifest.txt")).ok()?;
    let mut version = String::new();
    let mut arch = String::new();
    let mut files = Vec::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split_whitespace();
        let first = parts.next()?;
        match first {
            "version" => version = parts.next()?.to_string(),
            "arch" => arch = parts.next()?.to_string(),
            name => {
                let raw_len: u64 = parts.next()?.parse().ok()?;
                let sha256 = parts.next()?.to_string();
                files.push(Entry {
                    name: name.to_string(),
                    raw_len,
                    sha256,
                });
            }
        }
    }

    if version.is_empty() || arch.is_empty() || files.is_empty() {
        return None;
    }

    Some(Payload {
        dir: dir.to_path_buf(),
        version,
        arch,
        files,
    })
}
