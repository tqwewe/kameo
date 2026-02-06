use std::sync::atomic::{AtomicBool, Ordering};

use kameo_remote::AlignedBytes;
use thiserror::Error;

static TRUSTED_ARCHIVED: AtomicBool = AtomicBool::new(false);

/// Enable or disable trusted archived access at runtime.
///
/// Trusted mode only takes effect when the `trusted-archived` feature is enabled.
pub fn set_trusted_archived(enabled: bool) {
    TRUSTED_ARCHIVED.store(enabled, Ordering::SeqCst);
}

/// Returns true when trusted archived access is enabled and supported.
pub fn trusted_archived_enabled() -> bool {
    if !cfg!(feature = "trusted-archived") {
        return false;
    }
    TRUSTED_ARCHIVED.load(Ordering::Relaxed)
}

/// Returns true when the crate was compiled with trusted archived support.
pub fn trusted_archived_supported() -> bool {
    cfg!(feature = "trusted-archived")
}

#[derive(Debug, Error)]
pub enum ArchivedAccessError {
    #[error("misaligned payload buffer")]
    Misaligned,
    #[error(transparent)]
    Validation(#[from] rkyv::rancor::Error),
}

pub(crate) fn ensure_aligned(bytes: &[u8]) -> Result<(), ArchivedAccessError> {
    if (bytes.as_ptr() as usize) % kameo_remote::PAYLOAD_ALIGNMENT != 0 {
        return Err(ArchivedAccessError::Misaligned);
    }
    Ok(())
}

/// Access archived payloads with validated or trusted mode.
pub fn access_archived<T>(payload: &AlignedBytes) -> Result<&T, ArchivedAccessError>
where
    T: rkyv::Portable
        + for<'a> rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    if trusted_archived_enabled() {
        // CRITICAL_PATH: trusted archived access with alignment guard.
        ensure_aligned(payload.as_ref())?;
        // SAFETY: alignment verified and trusted mode explicitly enabled.
        Ok(unsafe { rkyv::access_unchecked::<T>(payload.as_ref()) })
    } else {
        Ok(rkyv::access::<T, rkyv::rancor::Error>(payload.as_ref())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trusted_archived_defaults_off() {
        set_trusted_archived(false);
        assert!(!trusted_archived_enabled());
    }

    #[cfg(feature = "trusted-archived")]
    #[test]
    fn trusted_archived_toggle_works() {
        set_trusted_archived(true);
        assert!(trusted_archived_enabled());
        set_trusted_archived(false);
        assert!(!trusted_archived_enabled());
    }

    #[cfg(feature = "trusted-archived")]
    #[test]
    fn trusted_archived_access_hits_guarded_path() {
        use rkyv::{Archive, Deserialize, Serialize};
        use rkyv::util::AlignedVec;

        #[derive(Archive, Serialize, Deserialize)]
        struct Payload {
            value: u32,
        }

        let payload = Payload { value: 42 };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&payload).expect("serialize");
        let mut aligned = AlignedVec::<kameo_remote::PAYLOAD_ALIGNMENT>::new();
        aligned.extend_from_slice(bytes.as_ref());
        let aligned_bytes = kameo_remote::AlignedBytes::from_aligned_vec(aligned);

        set_trusted_archived(true);
        let archived = access_archived::<rkyv::Archived<Payload>>(&aligned_bytes)
            .expect("trusted access");
        assert_eq!(archived.value, 42.into());
        set_trusted_archived(false);
    }

    #[test]
    fn alignment_guard_rejects_misaligned_slices() {
        let bytes = vec![0u8; 32];
        let misaligned = &bytes[1..];
        assert!(ensure_aligned(misaligned).is_err());
    }
}
