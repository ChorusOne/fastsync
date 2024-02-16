use std::collections::HashMap;
use std::io::{Read, Result, Write};
use std::net::TcpStream;
use std::os::fd::{AsRawFd, RawFd};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Instant;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;

const MAX_CHUNK_LEN: u64 = 4096 * 64;

/// Metadata about all the files we want to transfer.
///
/// We serialize the plan using Borsh, because it has a small Rust crate with
/// few dependencies, and it can do derive, and the wire format is similar to
/// what we’d write by hand anyway (count, then length-prefixed file names).
/// The plan is tiny compared to the data and we assume we’re not dealing with
/// malicious senders or receivers, so it doesn’t matter so much.
#[derive(BorshSerialize, BorshDeserialize, Debug)]
struct TransferPlan(Vec<FilePlan>);

/// Metadata about a file to transfer.
#[derive(BorshSerialize, BorshDeserialize, Debug)]
struct FilePlan {
    name: String,
    len: u64,
}

impl TransferPlan {
    /// Ask the user if they're okay (over)writing the target files.
    fn ask_confirm_receive(&self) -> Result<()> {
        println!("  SIZE_BYTES  FILENAME");
        for file in &self.0 {
            println!("{:>12}  {}", file.len, file.name);
        }
        print!("Receiving will overwrite existing files with those names. Continue? [y/N] ");
        let mut answer = String::new();
        std::io::stdout().flush()?;
        std::io::stdin().read_line(&mut answer)?;
        match &answer[..] {
            "y\n" => Ok(()),
            _ => Err(std::io::Error::other("Receive rejected by the user.")),
        }
    }
}

fn main() {
    // Skip the program name.
    let args: Vec<_> = std::env::args().skip(1).collect();

    match args.first().map(|s| &s[..]) {
        Some("send") if args.len() >= 3 => {
            let addr = &args[1];
            let fnames = &args[2..];
            main_send(addr, fnames).expect("Failed to send.");
        }
        Some("recv") if args.len() == 3 => {
            let addr = &args[1];
            let n_conn = &args[2];
            main_recv(addr, n_conn).expect("Failed to receive.");
        }
        _ => {
            eprintln!("Usage:");
            eprintln!("  fastsync send <listen-addr> <in-files...>");
            eprintln!("  fastsync recv <server-addr> <num-connections>");
        }
    }
}

fn print_progress(offset: u64, len: u64, start_time: Instant) {
    let secs_elapsed = start_time.elapsed().as_secs_f32();
    let percentage = (offset as f32) * 100.0 / (len as f32);
    let bytes_per_sec = (offset as f32) / secs_elapsed;
    let mb_per_sec = bytes_per_sec * 1e-6;
    let secs_left = (len - offset) as f32 / bytes_per_sec;
    let mins_left = secs_left / 60.0;
    println!(
        "[{offset} / {len}] {percentage:5.1}% {mb_per_sec:.2} MB/s, {mins_left:.1} minutes left",
    );
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
    pub fn send_one(&self, start_time: Instant, out: &mut TcpStream) -> Result<SendResult> {
        let offset = self.offset.fetch_add(MAX_CHUNK_LEN, Ordering::SeqCst);

        if offset >= self.len {
            // All zeros signals the end of the transmission.
            out.write_all(&[0u8; 20])?;
            return Ok(SendResult::Done);
        }

        print_progress(offset, self.len, start_time);

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

fn main_send(addr: &str, fnames: &[String]) -> Result<()> {
    let mut file_plans = Vec::new();
    let mut send_states = Vec::new();

    for fname in fnames {
        let file = std::fs::File::open(fname)?;
        let metadata = file.metadata()?;
        let file_plan = FilePlan {
            name: fname.clone(),
            len: metadata.len(),
        };
        let state = SendState {
            len: metadata.len(),
            offset: AtomicU64::new(0),
            in_fd: file.as_raw_fd(),
        };
        file_plans.push(file_plan);
        send_states.push(state);
    }

    let state_arc = Arc::new(send_states);
    let mut plan = Some(TransferPlan(file_plans));

    let mut push_threads = Vec::new();
    let listener = std::net::TcpListener::bind(addr)?;

    loop {
        let (mut stream, addr) = listener.accept()?;
        println!("Accepted connection from {addr}.");

        // If we are the first connection, then we need to send the plan first.
        if let Some(plan) = plan.take() {
            let mut buffer = Vec::new();
            plan.serialize(&mut buffer)
                .expect("Write to Vec<u8> does not fail.");
            stream.write_all(&buffer[..])?;
        }

        let state_clone = state_arc.clone();
        let push_thread = std::thread::spawn::<_, Result<()>>(move || {
            let start_time = Instant::now();
            loop {
                // TODO: Send the files one by one.
                //match state_clone.send_one(start_time, &mut stream)? {
                //    SendResult::Progress => continue,
                //    SendResult::Done => return Ok(()),
                //}
            }
        });
        push_threads.push(push_thread);
    }
}

struct Chunk {
    total_len: u64,
    offset: u64,
    data: Vec<u8>,
}

fn main_recv(addr: &str, n_conn: &str) -> Result<()> {
    let fname = "TODO: Fix";
    let n_connections: u32 = u32::from_str(n_conn).expect("Failed to parse number of connections.");

    let mut out_file = std::fs::File::create(fname)?;

    // Use a relatively small buffer; we should clear the buffer very quickly.
    let (sender, receiver) = mpsc::sync_channel::<Chunk>(16);

    let writer_thread = std::thread::spawn::<_, Result<()>>(move || {
        let start_time = Instant::now();
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
                print_progress(offset, total_len, start_time);

                if offset == total_len {
                    break;
                }
            }
        }

        Ok(())
    });

    // First we initiate one connection. The sender will send the plan over
    // that. We read it. Unbuffered, because we want to skip the buffer for the
    // remaining reads, but the header is tiny so it should be okay.
    let mut stream = TcpStream::connect(addr)?;
    let plan = TransferPlan::deserialize_reader(&mut stream)?;
    plan.ask_confirm_receive()?;

    // We make n threads that "pull" the data from a socket. The first socket we
    // already ahve, the transfer plan was sent on that one.
    let mut streams = vec![stream];
    for _ in 1..n_connections {
        streams.push(TcpStream::connect(addr)?);
    }

    let mut pull_threads = Vec::new();

    // We make n threads that "pull" the data from a socket.
    for mut stream in streams {
        let sender_i = sender.clone();
        let thread_pull = std::thread::spawn::<_, Result<()>>(move || {
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
                sender_i.send(chunk).expect("Failed to push new chunk.");
            }
            Ok(())
        });
        pull_threads.push(thread_pull);
    }

    // All of the threads have a copy of the sender, we no longer need the
    // original, and we need to drop it so that the wirter thread can exit
    // when all senders are done.
    std::mem::drop(sender);

    for pull_thread in pull_threads {
        pull_thread.join().expect("Failed to join pull thread.")?;
    }

    writer_thread.join().expect("Failed to join writer thread.")
}
