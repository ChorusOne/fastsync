use std::collections::HashMap;
use std::io::{Read, Result, Write};
use std::net::TcpStream;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Instant;

const MAX_CHUNK_LEN: u64 = 4096 * 64;

fn main() {
    // Skip the program name.
    let args: Vec<_> = std::env::args().skip(1).collect();

    match &args[..] {
        [cmd, addr, fname] if cmd == "send" => {
            main_send(addr, fname).expect("Failed to send.");
        }
        [cmd, addr, fname] if cmd == "recv" => {
            main_recv(addr, fname).expect("Failed to receive.");
        }
        _ => {
            eprintln!("Usage:");
            // TODO: Support multiple files, transfer the filenames.
            eprintln!("  fastsync send <listen-addr> <in-file>");
            eprintln!("  fastsync recv <server-addr> <out-file>");
            return;
        }
    }
}

struct SendState {
    len: u64,
    offset: AtomicU64,
    in_fd: RawFd,
}

enum SendResult {
    Done,
    Progress,
}

impl SendState {
    pub fn send_one(&self, out: &mut TcpStream) -> Result<SendResult> {
        let offset = self.offset.fetch_add(MAX_CHUNK_LEN, Ordering::SeqCst);

        if offset >= self.len {
            // All zeros signals the end of the transmission.
            out.write_all(&[0u8; 20])?;
            return Ok(SendResult::Done);
        }

        println!("[{} / {}]", offset, self.len);

        let end = if offset + MAX_CHUNK_LEN > self.len {
            self.len
        } else {
            offset + MAX_CHUNK_LEN
        };

        // We are going to do a few small writes, allow the kernel to buffer.
        out.set_nodelay(false)?;
        // TODO: Concat into buffer first, one fewere syscall.
        // Sending the len on every chunk is redundant, but it's not *that* many
        // bytes so for now keep it simple.
        out.write_all(&offset.to_le_bytes()[..])?;
        out.write_all(&self.len.to_le_bytes()[..])?;
        out.write_all(&((end - offset) as u32).to_le_bytes()[..])?;
        println!(
            "SEND-CHUNK {} {} {}",
            offset,
            self.len,
            (end - offset) as u32
        );

        let end = end as i64;
        let mut off = offset as i64;
        let out_fd = out.as_raw_fd();
        loop {
            let count = (end - off) as usize;
            let n_written = unsafe { libc::sendfile(out_fd, self.in_fd, &mut off, count) };
            if n_written < 0 {
                return Err(std::io::Error::last_os_error());
            }

            if off >= end {
                break;
            }
        }
        // Send now, stop buffering.
        out.set_nodelay(true)?;

        Ok(SendResult::Progress)
    }
}

fn main_send(addr: &str, fname: &str) -> Result<()> {
    let file = std::fs::File::open(fname)?;
    let metadata = file.metadata()?;

    let state = SendState {
        len: metadata.len(),
        offset: AtomicU64::new(0),
        in_fd: file.as_raw_fd(),
    };

    let listener = std::net::TcpListener::bind(addr)?;

    'outer: loop {
        let (mut stream, addr) = listener.accept()?;
        let start_time = Instant::now();
        println!("Accepted connection from {addr}.");
        loop {
            let bytes_sent = state.offset.load(Ordering::SeqCst);
            let duration = start_time.elapsed();
            let mbps = ((bytes_sent as f32) * 1e-6) / duration.as_secs_f32();
            println!("SPEED: {:.2} MB/s", mbps);

            match state.send_one(&mut stream)? {
                SendResult::Progress => continue,
                SendResult::Done => break 'outer,
            }
        }
    }
    println!("Sending complete.");

    Ok(())
}

struct Chunk {
    total_len: u64,
    offset: u64,
    data: Vec<u8>,
}

fn main_recv(addr: &str, fname: &str) -> Result<()> {
    let mut out_file = std::fs::File::create(fname)?;

    // Use a relatively small buffer; we should clear the buffer very quickly.
    let (sender, receiver) = mpsc::sync_channel::<Chunk>(16);

    let writer_thread = std::thread::spawn::<_, Result<()>>(move || {
        let mut pending = HashMap::new();
        let mut offset = 0;
        let mut total_len = 0;

        for chunk in receiver {
            // TODO: If it were only sent once, then we wouldn't have to check
            // for consistency.
            if total_len == 0 {
                total_len = chunk.total_len;
            } else {
                assert_eq!(total_len, chunk.total_len);
            }

            pending.insert(chunk.offset, chunk);

            // Write out all the chunks in the right order as far as we can.
            while let Some(chunk) = pending.remove(&offset) {
                out_file.write_all(&chunk.data[..])?;
                offset += chunk.data.len() as u64;
                println!("[{} / {}]", offset, total_len);

                if offset == total_len {
                    break;
                }
            }
        }

        Ok(())
    });

    let mut stream = TcpStream::connect(addr)?;
    loop {
        // Header is [offset: u64le, total_len: u64le, chunk_len: u32le].
        let mut buf = [0u8; 20];
        stream.read_exact(&mut buf)?;

        // All zeros signals the end of the transmission.
        if buf == [0u8; 20] {
            println!("END");
            break;
        }

        let offset = u64::from_le_bytes(buf[..8].try_into().unwrap());
        let total_len = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let chunk_len = u32::from_le_bytes(buf[16..].try_into().unwrap());
        println!("RECV-CHUNK {} {} {}", offset, total_len, chunk_len);

        assert!((chunk_len as u64) <= MAX_CHUNK_LEN);

        let mut data = Vec::with_capacity(chunk_len as usize);
        let mut limited = stream.take(chunk_len as u64);
        limited.read_to_end(&mut data)?;
        stream = limited.into_inner();

        let chunk = Chunk {
            offset,
            total_len,
            data,
        };
        sender.send(chunk).expect("Failed to push new chunk.");
    }
    std::mem::drop(sender);

    writer_thread.join().expect("Failed to join writer thread.")
}
