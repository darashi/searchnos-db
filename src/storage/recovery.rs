use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::warn;

use super::event::{EventPacket, error_with_path, read_event_packet};

pub(crate) fn recover_hot_events(
    file: &mut File,
    path: &Path,
) -> Result<Vec<EventPacket>, Box<dyn Error>> {
    file.seek(SeekFrom::Start(0))?;
    let mut packets = Vec::new();
    loop {
        let valid_bytes = file.stream_position()?;
        match read_event_packet(file) {
            Ok(Some(packet)) => packets.push(packet),
            Ok(None) => return Ok(packets),
            Err(err) => {
                let recoverable = err.downcast_ref::<io::Error>().is_some_and(|err| {
                    matches!(
                        err.kind(),
                        io::ErrorKind::UnexpectedEof | io::ErrorKind::InvalidData
                    )
                });
                if !recoverable {
                    return Err(error_with_path("read events", path, err));
                }

                let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
                let mut backup_name = std::ffi::OsString::from(format!("corrupt-{timestamp}-"));
                backup_name.push(path.file_name().ok_or("missing hot file name")?);
                let backup_path = path.with_file_name(backup_name);
                let mut backup = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&backup_path)
                    .map_err(|err| {
                        error_with_path("create hot events backup", &backup_path, err)
                    })?;
                file.seek(SeekFrom::Start(0))?;
                let original_bytes = io::copy(file, &mut backup)?;
                backup.sync_all()?;
                File::open(path.parent().ok_or("missing hot file directory")?)?.sync_all()?;

                file.set_len(valid_bytes)?;
                file.sync_all()?;
                warn!(
                    path = %path.display(),
                    backup_path = %backup_path.display(),
                    valid_events = packets.len(),
                    valid_bytes,
                    discarded_bytes = original_bytes - valid_bytes,
                    error = %err,
                    "recovered hot events up to the first damaged record"
                );
                return Ok(packets);
            }
        }
    }
}
