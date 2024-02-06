use std::io::{Result, Write};
use std::net::TcpStream;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicU64, Ordering};

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
        const CHUNK_LEN: u64 = 4096 * 64;
        let offset = self.offset.fetch_add(CHUNK_LEN, Ordering::SeqCst);

        if offset >= self.len {
            return Ok(SendResult::Done);
        }

        println!("[{} / {}]", offset, self.len);

        let end = if offset + CHUNK_LEN > self.len {
            self.len
        } else {
            offset + CHUNK_LEN
        };

        // We are going to do a few small writes, allow the kernel to buffer.
        out.set_nodelay(false)?;
        // TODO: Concat into buffer first, one fewere syscall.
        out.write_all(&offset.to_le_bytes()[..])?;
        out.write_all(&((end - offset) as u32).to_le_bytes()[..])?;

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

    'outer: for stream_res in listener.incoming() {
        let mut stream = stream_res?;
        loop {
            match state.send_one(&mut stream)? {
                SendResult::Progress => continue,
                SendResult::Done => break 'outer,
            }
        }
    }

    Ok(())
}

fn main_recv(addr: &str, fname: &str) -> Result<()> {
    eprintln!("TODO: Recv {addr} {fname}.");
    Ok(())
}
