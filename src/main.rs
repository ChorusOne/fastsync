use std::collections::HashMap;
use std::fs::File;
use std::io::{Error, ErrorKind, Read, Result, Write};
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
            _ => Err(Error::other("Receive rejected by the user.")),
        }
    }

    /// Crash if the plan contains absolute paths.
    ///
    /// We send the file names ahead of time and ask the user to confirm, but
    /// even then, if that list can include `/etc/ssh/sshd_config` or something,
    /// that could be pretty disastrous. Only allow relative paths.
    fn assert_paths_relative(&self) {
        for file in &self.0 {
            assert!(
                !file.name.starts_with("/"),
                "Transferring files with an absolute path name is not allowed.",
            );
        }
    }
}

/// The index of a file in the transfer plan.
#[derive(BorshDeserialize, BorshSerialize, Copy, Clone, Debug, Eq, Hash, PartialEq)]
struct FileId(u16);

impl FileId {
    fn from_usize(i: usize) -> FileId {
        assert!(i < u16::MAX as usize, "Can transfer at most 2^16 files.");
        FileId(i as _)
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
    id: FileId,
    len: u64,
    offset: AtomicU64,
    in_fd: RawFd,
}

enum SendResult {
    Done,
    Progress,
}

/// Metadata about a chunk of data that follows.
///
/// The Borsh-generated representation of this is zero-overhead (14 bytes).
#[derive(BorshDeserialize, BorshSerialize, Debug)]
struct ChunkHeader {
    /// Which file is the chunk from?
    file_id: FileId,

    /// Byte offset in the file where the chunk starts.
    offset: u64,

    /// Length of the chunk in bytes.
    len: u32,
}

impl ChunkHeader {
    fn to_bytes(&self) -> [u8; 14] {
        let mut buffer = [0_u8; 14];
        let mut cursor = std::io::Cursor::new(&mut buffer[..]);
        self.serialize(&mut cursor)
            .expect("Writing to memory never fails.");
        buffer
    }
}

impl SendState {
    pub fn send_one(&self, start_time: Instant, out: &mut TcpStream) -> Result<SendResult> {
        let offset = self.offset.fetch_add(MAX_CHUNK_LEN, Ordering::SeqCst);

        if offset >= self.len {
            return Ok(SendResult::Done);
        }

        print_progress(offset, self.len, start_time);

        let end = if offset + MAX_CHUNK_LEN > self.len {
            self.len
        } else {
            offset + MAX_CHUNK_LEN
        };

        let header = ChunkHeader {
            file_id: self.id,
            offset,
            len: u32::try_from(end - offset).expect("Chunks are smaller that 4 GiB."),
        };
        out.write_all(&header.to_bytes()[..])?;
        println!("SEND-CHUNK {:?}", header);

        let end = end as i64;
        let mut off = offset as i64;
        let out_fd = out.as_raw_fd();
        loop {
            let count = (end - off) as usize;
            let n_written = unsafe { libc::sendfile(out_fd, self.in_fd, &mut off, count) };
            if n_written < 0 {
                return Err(Error::last_os_error());
            }

            if off >= end {
                break;
            }
        }

        Ok(SendResult::Progress)
    }
}

fn main_send(addr: &str, fnames: &[String]) -> Result<()> {
    let mut plan = TransferPlan(Vec::new());
    let mut send_states = Vec::new();

    for (i, fname) in fnames.iter().enumerate() {
        let file = std::fs::File::open(fname)?;
        let metadata = file.metadata()?;
        let file_plan = FilePlan {
            name: fname.clone(),
            len: metadata.len(),
        };
        let state = SendState {
            id: FileId::from_usize(i),
            len: metadata.len(),
            offset: AtomicU64::new(0),
            in_fd: file.as_raw_fd(),
        };
        plan.0.push(file_plan);
        send_states.push(state);
    }

    plan.assert_paths_relative();

    let state_arc = Arc::new(send_states);
    let mut plan = Some(plan);

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
            // All the threads iterate through all the files one by one, so all
            // the threads collaborate on sending the first one, then the second
            // one, etc.
            'files: for file in state_clone.iter() {
                'chunks: loop {
                    match file.send_one(start_time, &mut stream)? {
                        SendResult::Progress => continue 'chunks,
                        SendResult::Done => continue 'files,
                    }
                }
            }

            Ok(())
        });
        push_threads.push(push_thread);
    }
}

struct Chunk {
    file_id: FileId,
    offset: u64,
    data: Vec<u8>,
}

struct FileReceiver {
    fname: String,

    /// The file we’re writing to, if we have started writing.
    ///
    /// We don’t open the file immediately so we don’t create a zero-sized file
    /// when a transfer fails. We only open the file after we have at least some
    /// data for it.
    out_file: Option<File>,

    /// Chunks that we cannot yet write because a preceding chunk has not yet arrived.
    pending: HashMap<u64, Chunk>,

    /// How many bytes we have written so far.
    offset: u64,

    /// How many bytes we should receive.
    total_len: u64,
}

impl FileReceiver {
    fn new(plan: FilePlan) -> FileReceiver {
        FileReceiver {
            fname: plan.name,
            out_file: None,
            pending: HashMap::new(),
            offset: 0,
            total_len: plan.len,
        }
    }

    /// Write or buffer a chunk that we received for this file.
    fn handle_chunk(&mut self, chunk: Chunk) -> Result<()> {
        let mut out_file = match self.out_file.take() {
            None => File::create(&self.fname)?,
            Some(f) => f,
        };
        self.pending.insert(chunk.offset, chunk);

        // Write out all the chunks in the right order as far as we can.
        while let Some(chunk) = self.pending.remove(&self.offset) {
            out_file.write_all(&chunk.data[..])?;
            self.offset += chunk.data.len() as u64;
        }

        self.out_file = Some(out_file);
        Ok(())
    }
}

fn main_recv(addr: &str, n_conn: &str) -> Result<()> {
    let n_connections: u32 = u32::from_str(n_conn).expect("Failed to parse number of connections.");

    // First we initiate one connection. The sender will send the plan over
    // that. We read it. Unbuffered, because we want to skip the buffer for the
    // remaining reads, but the header is tiny so it should be okay.
    let mut stream = TcpStream::connect(addr)?;
    let plan = TransferPlan::deserialize_reader(&mut stream)?;
    plan.ask_confirm_receive()?;

    // Use a relatively small buffer; we should clear the buffer very quickly.
    let (sender, receiver) = mpsc::sync_channel::<Chunk>(16);

    let writer_thread = std::thread::spawn::<_, Result<()>>(move || {
        let total_len: u64 = plan.0.iter().map(|f| f.len).sum();
        let mut files: Vec<_> = plan.0.into_iter().map(FileReceiver::new).collect();

        let start_time = Instant::now();
        let mut bytes_received: u64 = 0;

        for chunk in receiver {
            let file = &mut files[chunk.file_id.0 as usize];
            bytes_received += chunk.data.len() as u64;
            file.handle_chunk(chunk)?;
            print_progress(bytes_received, total_len, start_time);
        }

        Ok(())
    });

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
                // Read a chunk header. If we hit EOF, that is not an error, it
                // means that the sender has nothing more to send so we can just
                // exit here.
                let mut buf = [0u8; 14];
                match stream.read_exact(&mut buf) {
                    Ok(..) => {}
                    Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                    Err(err) => return Err(err),
                };

                let header = ChunkHeader::try_from_slice(&buf[..])?;
                assert!((header.len as u64) <= MAX_CHUNK_LEN);
                println!("RECV-CHUNK {:?}", header);

                let mut data = Vec::with_capacity(header.len as usize);
                let mut limited = stream.take(header.len as u64);
                limited.read_to_end(&mut data)?;
                stream = limited.into_inner();

                let chunk = Chunk {
                    file_id: header.file_id,
                    offset: header.offset,
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
