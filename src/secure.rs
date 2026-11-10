//! Encryption at rest for the stored Backblaze credentials, using Windows DPAPI.
//!
//! DPAPI ties the ciphertext to the signed-in Windows user, so the files are useless
//! if copied to another account or machine, and there is no key for the app to manage
//! or leak. It protects against another user, a stolen disk and a backup that leaves
//! the machine. It does not protect against malware already running as the same user,
//! which no file-based scheme can.
use anyhow::{anyhow, Result};
use std::ptr;
use windows_sys::Win32::Foundation::LocalFree;
use windows_sys::Win32::Security::Cryptography::{
    CryptProtectData, CryptUnprotectData, CRYPT_INTEGER_BLOB,
};

fn blob_of(data: &[u8]) -> CRYPT_INTEGER_BLOB {
    CRYPT_INTEGER_BLOB {
        cbData: data.len() as u32,
        pbData: data.as_ptr() as *mut u8,
    }
}

/// Copies the bytes DPAPI allocated and frees the original.
fn take(out: CRYPT_INTEGER_BLOB) -> Vec<u8> {
    // SAFETY: on success DPAPI returns a LocalAlloc'd buffer of `cbData` bytes.
    let bytes = unsafe { std::slice::from_raw_parts(out.pbData, out.cbData as usize) }.to_vec();
    unsafe { LocalFree(out.pbData as _) };
    bytes
}

pub fn protect(plain: &[u8]) -> Result<Vec<u8>> {
    let input = blob_of(plain);
    let mut out = CRYPT_INTEGER_BLOB { cbData: 0, pbData: ptr::null_mut() };
    // SAFETY: `input` borrows `plain` for the duration of the call; `out` is written by DPAPI.
    let ok = unsafe {
        CryptProtectData(&input, ptr::null(), ptr::null(), ptr::null(), ptr::null(), 0, &mut out)
    };
    if ok == 0 {
        return Err(anyhow!("CryptProtectData failed: {}", std::io::Error::last_os_error()));
    }
    Ok(take(out))
}

pub fn unprotect(sealed: &[u8]) -> Result<Vec<u8>> {
    let input = blob_of(sealed);
    let mut out = CRYPT_INTEGER_BLOB { cbData: 0, pbData: ptr::null_mut() };
    // SAFETY: as in `protect`.
    let ok = unsafe {
        CryptUnprotectData(&input, ptr::null_mut(), ptr::null(), ptr::null(), ptr::null(), 0, &mut out)
    };
    if ok == 0 {
        return Err(anyhow!("CryptUnprotectData failed: {}", std::io::Error::last_os_error()));
    }
    Ok(take(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let sealed = protect(b"application-key-123").unwrap();
        assert_eq!(unprotect(&sealed).unwrap(), b"application-key-123");
    }

    #[test]
    fn ciphertext_does_not_contain_the_plaintext() {
        let sealed = protect(b"application-key-123").unwrap();
        assert!(!sealed.windows(19).any(|w| w == b"application-key-123"));
    }

    #[test]
    fn tampered_ciphertext_is_rejected() {
        let mut sealed = protect(b"application-key-123").unwrap();
        let last = sealed.len() - 1;
        sealed[last] ^= 0xff;
        assert!(unprotect(&sealed).is_err());
    }
}
