//! Byte-free, per-reader presence on the physical database file. Linux OFD
//! locks survive the load flock and are released with the last reader guard.

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::sync::Arc;

const WIDTH: i64 = 1 << 60;

fn offsets(identity: [u8; 16]) -> [i64; 2] {
    let first = u64::from_le_bytes(identity[..8].try_into().unwrap()) as i64 & (WIDTH - 1);
    let second = u64::from_le_bytes(identity[8..].try_into().unwrap()) as i64 & (WIDTH - 1);
    [WIDTH + first, 2 * WIDTH + second]
}

fn change(file: &File, offset: i64, kind: libc::c_short) -> io::Result<()> {
    let mut lock = libc::flock {
        l_type: kind,
        l_whence: libc::SEEK_SET as libc::c_short,
        l_start: offset,
        l_len: 1,
        l_pid: 0,
    };
    // SAFETY: lock is initialized and the descriptor remains owned by file.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_SETLK, &mut lock) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(super) struct ReaderPresenceGuard {
    file: Arc<File>,
    offsets: [i64; 2],
}

impl ReaderPresenceGuard {
    pub(super) fn acquire(file: Arc<File>, identity: [u8; 16]) -> io::Result<Self> {
        let offsets = offsets(identity);
        change(&file, offsets[0], libc::F_RDLCK as libc::c_short)?;
        if let Err(error) = change(&file, offsets[1], libc::F_RDLCK as libc::c_short) {
            let _ = change(&file, offsets[0], libc::F_UNLCK as libc::c_short);
            return Err(error);
        }
        Ok(Self { file, offsets })
    }
}

impl Drop for ReaderPresenceGuard {
    fn drop(&mut self) {
        for offset in self.offsets {
            let _ = change(&self.file, offset, libc::F_UNLCK as libc::c_short);
        }
    }
}

/// Test every gap around authenticated reader offsets. F_OFD_GETLK reports
/// one arbitrary conflict in a queried range, so advancing past a reported
/// known offset could miss an unknown lock before it.
pub(super) fn has_unknown(
    file: &File,
    handles: impl IntoIterator<Item = [u8; 16]>,
) -> io::Result<bool> {
    let mut known: Vec<i64> = handles.into_iter().flat_map(offsets).collect();
    known.sort_unstable();
    known.dedup();
    scan_gaps(&known, |start, len| {
        let mut lock = libc::flock {
            l_type: libc::F_WRLCK as libc::c_short,
            l_whence: libc::SEEK_SET as libc::c_short,
            l_start: start,
            l_len: len,
            l_pid: 0,
        };
        // SAFETY: lock is initialized and the descriptor remains owned by file.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_GETLK, &mut lock) } == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok((lock.l_type != libc::F_UNLCK as libc::c_short).then_some(lock.l_start))
    })
}

fn scan_gaps(
    known: &[i64],
    mut probe: impl FnMut(i64, i64) -> io::Result<Option<i64>>,
) -> io::Result<bool> {
    for base in [WIDTH, 2 * WIDTH] {
        let end = base + WIDTH;
        let mut start = base;
        for boundary in known
            .iter()
            .copied()
            .filter(|offset| *offset >= base && *offset < end)
            .chain(std::iter::once(end))
        {
            if start == boundary {
                start = boundary + 1;
                continue;
            }
            if probe(start, boundary - start)?.is_some() {
                return Ok(true);
            }
            start = boundary + 1;
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(offset: u64) -> [u8; 16] {
        let mut identity = [0; 16];
        identity[..8].copy_from_slice(&offset.to_le_bytes());
        identity[8..].copy_from_slice(&offset.to_le_bytes());
        identity
    }

    #[test]
    fn authenticated_known_reader_is_not_unknown_in_either_range() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("presence.db");
        let file = File::create(&path).unwrap();
        let known = identity(200);
        let _reader =
            ReaderPresenceGuard::acquire(Arc::new(File::open(&path).unwrap()), known).unwrap();
        assert!(!has_unknown(&file, [known]).unwrap());
        assert!(has_unknown(&file, std::iter::empty()).unwrap());
    }

    #[test]
    fn later_known_lock_cannot_hide_an_earlier_unknown_lock_in_either_range() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("presence.db");
        let file = File::create(&path).unwrap();
        let known = identity(200);
        let _known_reader =
            ReaderPresenceGuard::acquire(Arc::new(File::open(&path).unwrap()), known).unwrap();
        let unknown_file = Arc::new(File::open(&path).unwrap());
        let unknown_reader =
            ReaderPresenceGuard::acquire(Arc::clone(&unknown_file), identity(100)).unwrap();
        change(
            &unknown_file,
            unknown_reader.offsets[1],
            libc::F_UNLCK as libc::c_short,
        )
        .unwrap();
        assert!(
            has_unknown(&file, [known]).unwrap(),
            "first range must be covered"
        );
        change(
            &unknown_file,
            unknown_reader.offsets[1],
            libc::F_RDLCK as libc::c_short,
        )
        .unwrap();
        change(
            &unknown_file,
            unknown_reader.offsets[0],
            libc::F_UNLCK as libc::c_short,
        )
        .unwrap();
        assert!(
            has_unknown(&file, [known]).unwrap(),
            "second range must be covered"
        );
        change(
            &unknown_file,
            unknown_reader.offsets[1],
            libc::F_UNLCK as libc::c_short,
        )
        .unwrap();
        assert!(!has_unknown(&file, [known]).unwrap());
    }

    #[test]
    fn adverse_probe_ordering_still_covers_the_earlier_unknown_offset() {
        let known_offset = WIDTH + 200;
        let unknown_offset = WIDTH + 100;
        // A whole-range GETLK may choose the known lock even when the unknown
        // lock is earlier. The scanner must never offer that pair to one probe.
        let probe = |start: i64, len: i64| {
            let end = start + len;
            if (start..end).contains(&known_offset) {
                Ok(Some(known_offset))
            } else if (start..end).contains(&unknown_offset) {
                Ok(Some(unknown_offset))
            } else {
                Ok(None)
            }
        };
        assert_eq!(probe(WIDTH, WIDTH).unwrap(), Some(known_offset));
        assert!(scan_gaps(&[known_offset], probe).unwrap());
    }
}
