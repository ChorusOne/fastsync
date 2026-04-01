// Fastsync -- Send files quickly by leveraging multiple TCP connections.
// Copyright 2024 Chorus One

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// A copy of the License has been included in the root of the repository.

mod ratelimiter;

use std::collections::HashMap;
use std::fs::File;
use std::io::{Error, ErrorKind, Read, Result, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;
use walkdir::WalkDir;

use bpaf::{construct, long, positional, OptionParser, Parser};

use crate::ratelimiter::RateLimiter;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;

#[derive(Debug)]
enum FastsyncError {
    #[allow(dead_code)] // allow for debug output
    AppError(String),
    #[allow(dead_code)] // allow for debug output
    IoError(std::io::Error),
}

impl From<std::io::Error> for FastsyncError {
    fn from(err: std::io::Error) -> FastsyncError {
        FastsyncError::IoError(err)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Verbosity {
    Silent,
    Verbose,
}

#[derive(Debug, Clone)]
enum Command {
    Send {
        verbosity: Verbosity,
        listen_addr: SocketAddr,
        max_bandwidth: Option<u64>,
        fnames: Vec<String>,
    },
    Recv {
        verbosity: Verbosity,
        server_addr: SocketAddr,
        n_conn: u32,
    },
}

const WIRE_PROTO_VERSION: u16 = 2;
const MAX_CHUNK_LEN: u64 = 4096 * 64;

/// Metadata about all the files we want to transfer.
///
/// We serialize the plan using Borsh, because it has a small Rust crate with
/// few dependencies, and it can do derive, and the wire format is similar to
/// what we’d write by hand anyway (count, then length-prefixed file names).
/// The plan is tiny compared to the data and we assume we’re not dealing with
/// malicious senders or receivers, so it doesn’t matter so much.
#[derive(BorshSerialize, BorshDeserialize, Debug)]
struct TransferPlan {
    proto_version: u16,
    files: Vec<FilePlan>,
}

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
        for file in &self.files {
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
        for file in &self.files {
            assert!(
                !file.name.starts_with('/'),
                "Transferring files with an absolute path name is not allowed.",
            );
        }
    }
}

/// The index of a file in the transfer plan.
#[derive(BorshDeserialize, BorshSerialize, Copy, Clone, Debug, Eq, Hash, PartialEq)]
struct FileId(u32);

impl FileId {
    fn from_usize(i: usize) -> FileId {
        assert!(i < u32::MAX as usize, "Can transfer at most 2^32 files.");
        FileId(i as _)
    }
}

#[derive(PartialEq)]
enum WriteMode {
    AskConfirm,
    #[allow(dead_code)]
    Force,
}
enum SenderEvent {
    Listening(u16),
}

fn parse_socket_addr(s: String) -> std::result::Result<SocketAddr, String> {
    SocketAddr::from_str(&s).map_err(|e| format!("Invalid address '{}': {}", s, e))
}

fn cli() -> OptionParser<Command> {
    #[inline]
    fn verbosity() -> impl Parser<Verbosity> {
        bpaf::short('v')
            .long("verbose")
            .help("Enable verbose debug output")
            .flag(Verbosity::Verbose, Verbosity::Silent)
    }
    let send_parser = {
        let max_bandwidth = long("max-bandwidth-mbps")
            .help("Specify the maximum bandwidth to use over a 1 second sliding window, in MB/s. If unspecified, there will be no limit")
            .argument::<u64>("MBPS")
            .optional();

        let listen_addr = positional::<String>("LISTEN_ADDR")
            .help("Address (IP and port) for the sending side to bind to and listen for receivers. This should be the address of a Wireguard interface if you care about confidentiality. E.g. '100.71.154.83:7999'")
            .parse(parse_socket_addr);

        let fnames = positional::<String>("FILES")
            .help("Paths of files to send. Input file paths need to be relative. This is a safety measure to make it harder to accidentally overwrite files in /etc and the like on the receiving end")
            .some("At least one file must be specified");

        construct!(Command::Send {
            verbosity(),
            max_bandwidth,
            listen_addr,
            fnames
        })
        .to_options()
        .command("send")
        .help("Send files to a receiver")
    };

    let recv_parser = {
        let server_addr = positional::<String>("SERVER_ADDR")
            .help("The address (IP and port) that the sender is listening on. E.g. '100.71.154.83:7999'")
            .parse(parse_socket_addr);

        let n_conn = positional::<u32>("NUM_STREAMS")
            .help("The number of TCP streams to open. For a value of 1, Fastsync behaves very similar to 'netcat'. With higher values, Fastsync leverages the fact that file chunks don't need to arrive in order to avoid the head-of-line blocking of a single connection. You should experiment to find the best value, going from 1 to 4 is usually helpful, going from 16 to 32 is probably overkill");

        construct!(Command::Recv {
            verbosity(),
            server_addr,
            n_conn
        })
        .to_options()
        .command("recv")
        .help("Receive files from a receiver")
    };

    construct!([send_parser, recv_parser])
        .to_options()
        .descr("Fastsync -- Transfer files over multiple TCP streams")
        .version(env!("CARGO_PKG_VERSION"))
}

fn main() {
    let (events_tx, events_rx) = std::sync::mpsc::channel::<SenderEvent>();

    match cli().run() {
        Command::Send {
            verbosity,
            listen_addr,
            max_bandwidth,
            fnames,
        } => {
            main_send(
                listen_addr,
                &fnames,
                WIRE_PROTO_VERSION,
                events_tx,
                max_bandwidth,
                verbosity,
            )
            .expect("Failed to send.");
        }
        Command::Recv {
            verbosity,
            server_addr,
            n_conn,
        } => {
            main_recv(
                server_addr,
                n_conn,
                WriteMode::AskConfirm,
                WIRE_PROTO_VERSION,
                verbosity,
            )
            .expect("Failed to receive.");
        }
    }
    drop(events_rx);
}

fn print_progress(offset: u64, len: u64, start_time: Instant) -> std::io::Result<()> {
    let secs_elapsed = start_time.elapsed().as_secs_f32();
    let percentage = (offset as f32) * 100.0 / (len as f32);
    let bytes_per_sec = (offset as f32) / secs_elapsed;
    let mb_per_sec = bytes_per_sec * 1e-6;
    let secs_left = (len - offset) as f32 / bytes_per_sec;
    let hours = (secs_left / 3600.0) as u32;
    let mins = ((secs_left % 3600.0) / 60.0) as u32;

    let offset_gb = offset as f64 / 1_000_000_000.0;
    let len_gb = len as f64 / 1_000_000_000.0;

    let mut stdout = std::io::stdout().lock();

    // Clear rest of line, add a newline and move back to progress line
    // In normal mode, this shows as one constantly changing line,
    // while not interfering with verbose mode logs.
    let escape_seq = "\x1b[K\n\x1b[A";
    write!(
        stdout,
        "\r[{:.2} GB / {:.2} GB] {percentage:2.1}% {mb_per_sec:.2} MB/s, {hours}h {mins}m left{escape_seq}",
        offset_gb, len_gb
    )?;
    stdout.flush()
}

enum SendStateInner {
    Pending { fname: PathBuf },
    InProgress { file: File, offset: u64 },
    Done,
}

struct SendState {
    id: FileId,
    len: u64,
    state: parking_lot::Mutex<SendStateInner>,
}

enum SendResult {
    Done,
    FileVanished { fname: PathBuf },
    Progress { bytes_sent: u64 },
}

/// Metadata about a chunk of data that follows.
///
/// The Borsh-generated representation of this is zero-overhead (16 bytes).
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
    fn to_bytes(&self) -> [u8; 16] {
        let mut buffer = [0_u8; 16];
        let mut cursor = std::io::Cursor::new(&mut buffer[..]);
        self.serialize(&mut cursor)
            .expect("Writing to memory never fails.");
        buffer
    }
}

impl SendState {
    pub fn send_one(&self, out: &mut TcpStream, verbosity: Verbosity) -> Result<SendResult> {
        // By deferring the opening of the file descriptor to this point,
        // we effectively limit the amount of open files to the amount of send threads.
        // However, this now introduces the possibility of files getting deleted between
        // getting listed and their turn for transfer.
        // A vanishing file is expected, it is not a transfer-terminating event.
        let mut state = self.state.lock();
        let (offset, in_fd) = match *state {
            SendStateInner::Pending { ref fname } => {
                let res = match std::fs::File::open(fname) {
                    Ok(f) => f,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        return Ok(SendResult::FileVanished {
                            fname: fname.clone(),
                        });
                    }
                    Err(e) => return Err(e),
                };
                let fd = res.as_raw_fd();
                *state = SendStateInner::InProgress {
                    file: res,
                    offset: 0,
                };
                (0, fd)
            }
            SendStateInner::Done => {
                return Ok(SendResult::Done);
            }
            SendStateInner::InProgress {
                ref file,
                ref mut offset,
            } => {
                *offset += MAX_CHUNK_LEN;
                (*offset, file.as_raw_fd())
            }
        };

        let end = self.len.min(offset + MAX_CHUNK_LEN);

        if offset >= self.len || offset >= end {
            *state = SendStateInner::Done;
            return Ok(SendResult::Done);
        }
        // Drop the lock while sending -- so that multiple threads can
        // send data from the same file at once
        std::mem::drop(state);

        let header = ChunkHeader {
            file_id: self.id,
            offset,
            len: u32::try_from(end - offset).expect("Chunks are smaller than 4 GiB."),
        };
        out.write_all(&header.to_bytes()[..])?;
        if Verbosity::Verbose == verbosity {
            println!(
                "SEND-CHUNK {:?} header_len={}",
                header,
                borsh::to_vec(&header)?.len()
            );
        }

        let end = end as i64;
        let mut off = offset as i64;
        let mut total_written: u64 = 0;
        let out_fd = out.as_raw_fd();
        while off < end {
            let count = (end - off) as usize;
            // Note, sendfile on Linux advances the offset by the number of bytes
            // written so we should not increment `off` ourselves.
            #[cfg(target_os = "linux")]
            let n_written = {
                let n = unsafe { libc::sendfile64(out_fd, in_fd, &mut off, count) };
                if n < 0 {
                    return Err(Error::last_os_error());
                }
                n as u64
            };

            #[cfg(target_os = "macos")]
            let n_written = {
                let mut len = count as libc::off_t;
                let ret = unsafe {
                    libc::sendfile(in_fd, out_fd, off, &mut len, std::ptr::null_mut(), 0)
                };
                if ret < 0 {
                    return Err(Error::last_os_error());
                }
                // On macOS, sendfile does not advance the offset, so we have to do it ourselves.
                off += len;
                len as u64
            };

            total_written += n_written;
        }

        Ok(SendResult::Progress {
            bytes_sent: total_written,
        })
    }
}

fn all_filenames_from_path_names(fnames: &[String]) -> Result<Vec<String>> {
    let mut all_files = Vec::with_capacity(fnames.len());

    for fname in fnames {
        let metadata = std::fs::metadata(fname)
            .map_err(|e| Error::new(e.kind(), format!("Unable to access '{fname}'")))?;

        if metadata.is_file() {
            all_files.push(fname.to_owned());
        } else if metadata.is_dir() {
            all_files.extend(
                WalkDir::new(fname)
                    .into_iter()
                    .filter_map(|x| x.ok())
                    .filter(|e| e.file_type().is_file())
                    .filter_map(|e| e.path().to_str().map(String::from)),
            );
        }
    }
    Ok(all_files)
}

fn main_send(
    addr: SocketAddr,
    fnames: &[String],
    protocol_version: u16,
    sender_events: std::sync::mpsc::Sender<SenderEvent>,
    max_bandwidth_mbps: Option<u64>,
    verbosity: Verbosity,
) -> std::result::Result<(), FastsyncError> {
    let mut plan = TransferPlan {
        proto_version: protocol_version,
        files: Vec::new(),
    };
    let mut send_states = Vec::new();
    let mut total_size = 0;

    for (i, fname) in all_filenames_from_path_names(fnames)?.iter().enumerate() {
        let metadata = std::fs::metadata(fname)?;
        let file_len = metadata.len();
        total_size += file_len;
        let file_plan = FilePlan {
            name: fname.clone(),
            len: file_len,
        };
        let state = SendState {
            id: FileId::from_usize(i),
            len: file_len,
            state: parking_lot::Mutex::new(SendStateInner::Pending {
                fname: fname.into(),
            }),
        };
        plan.files.push(file_plan);
        send_states.push(state);
    }

    plan.assert_paths_relative();

    let state_arc = Arc::new(send_states);
    let mut plan = Some(plan);

    let mut push_threads = Vec::new();
    let listener = std::net::TcpListener::bind(addr)?;

    let total_bytes_sent = Arc::new(AtomicU64::new(0));

    println!("Waiting for the receiver ...");
    sender_events
        .send(SenderEvent::Listening(
            listener.local_addr().unwrap().port(),
        ))
        .expect("Listener should not exit before the sender.");

    let limiter_mutex = Arc::new(Mutex::new(Option::<RateLimiter>::None));

    if let Some(mbps) = max_bandwidth_mbps {
        let ratelimiter = RateLimiter::new(mbps, MAX_CHUNK_LEN, Instant::now());
        _ = limiter_mutex.lock().unwrap().insert(ratelimiter);
    }

    let (error_tx, error_rx) = std::sync::mpsc::channel::<FastsyncError>();

    let mut start_time_opt: Option<Instant> = None;
    loop {
        if let Some(err) = error_rx.try_recv().ok() {
            return Err(err);
        }

        let (mut stream, addr) = listener.accept()?;
        let start_time = *start_time_opt.get_or_insert_with(Instant::now);
        if Verbosity::Verbose == verbosity {
            println!("Accepted connection from {addr}.");
        }

        // If we are the first connection, then we need to send the plan first.
        if let Some(plan) = plan.take() {
            let mut buffer = Vec::new();
            plan.serialize(&mut buffer)
                .expect("Write to Vec<u8> does not fail.");
            stream.write_all(&buffer[..])?;
            println!("Waiting for the receiver to accept ...");
        }

        // If all files have been transferred completely, then we are done.
        // Stop the listener, don't send anything over our new connection.
        let is_done = state_arc
            .iter()
            .all(|f| matches!(*f.state.lock(), SendStateInner::Done));
        if is_done {
            break;
        }

        let state_clone = state_arc.clone();
        let limiter_mutex_2 = limiter_mutex.clone();
        let total_bytes_sent_clone = total_bytes_sent.clone();
        let thread_error_tx = error_tx.clone();
        let push_thread = std::thread::spawn(move || {
            // All the threads iterate through all the files one by one, so all
            // the threads collaborate on sending the first one, then the second
            // one, etc.

            'files: for file in state_clone.iter() {
                'chunks: loop {
                    let mut limiter_mutex = limiter_mutex_2.lock().unwrap();
                    let mut opt_ratelimiter = limiter_mutex.as_mut();
                    if let Some(ref mut ratelimiter) = opt_ratelimiter {
                        let to_wait =
                            ratelimiter.time_until_bytes_available(Instant::now(), MAX_CHUNK_LEN);
                        // if to_wait is None, we've requested to send more than the bucket's max
                        // capacity, which is a programming error. Crash the program.
                        std::thread::sleep(to_wait.unwrap());
                    }
                    match file.send_one(&mut stream, verbosity) {
                        Ok(SendResult::FileVanished { fname }) => {
                            let error_msg = format!(
                                "File {:?} vanished during transfer, cannot perform full transfer.",
                                fname
                            );
                            match thread_error_tx.send(FastsyncError::AppError(error_msg.clone())) {
                                Ok(_) => {}
                                Err(_) => {
                                    // If other thread reported error already the channel will
                                    // be closed, in this case just log error and exit.
                                    println!("Error channel closed already. Thread encountered an error: {error_msg}");
                                }
                            }
                            return;
                        }
                        Ok(SendResult::Progress {
                            bytes_sent: bytes_written,
                        }) => {
                            let prev_total_bytes_sent =
                                total_bytes_sent_clone.fetch_add(bytes_written, Ordering::Relaxed);
                            print_progress(
                                prev_total_bytes_sent + bytes_written,
                                total_size,
                                start_time,
                            )
                            .ok();
                            if let Some(ref mut ratelimiter) = opt_ratelimiter {
                                ratelimiter.consume_bytes(Instant::now(), bytes_written);
                            }
                            continue 'chunks;
                        }
                        Ok(SendResult::Done) => continue 'files,
                        Err(err) => {
                            let error_msg = format!("Failed to send: {err}");
                            thread_error_tx
                                .send(FastsyncError::AppError(error_msg.clone()))
                                .expect("expected error channel to be open");
                            return;
                        }
                    }
                }
            }
        });
        push_threads.push(push_thread);
    }

    // For a long transfer, the listener loop exists when the receiver signals
    // that it received everything by connecting one final time. But it can also
    // happen that we pushed everything before the receiver was even done
    // spawning connections, so either way, we need to wait for the push threads
    // to finish sending.
    for push_thread in push_threads {
        push_thread.join().expect("Failed to wait for push thread.");
    }

    // Before we exit, check if any of the threads reported an error
    if let Some(err) = error_rx.try_recv().ok() {
        return Err(err);
    }

    Ok(())
}

struct Chunk {
    file_id: FileId,
    offset: u64,
    data: Vec<u8>,
}

struct FileReceiver {
    fname: String,

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
            None => {
                let path: &Path = self.fname.as_ref();
                if let Some(dir) = path.parent() {
                    std::fs::create_dir_all(dir)?;
                }
                let file = File::create(path)?;

                // Resize the file to its final size already:
                // * So that the file system can do a better job of allocating
                //   a single extent for it, and it doesn't have to fragment
                //   the file.
                // * If we run out of space, we learn about that before we waste
                //   time on the transfer (although then maybe we should do it
                //   before we receive a chunk after all?).
                // This can make debugging a bit harder, because when you look
                // at just the file size you might think it's fully transferred.
                file.set_len(self.total_len)?;

                file
            }
            Some(f) => f,
        };
        self.pending.insert(chunk.offset, chunk);

        // Write out all the chunks in the right order as far as we can.
        while let Some(chunk) = self.pending.remove(&self.offset) {
            out_file.write_all(&chunk.data[..])?;
            self.offset += chunk.data.len() as u64;
        }

        if self.offset < self.total_len {
            self.out_file = Some(out_file);
            // Only keep the file open as long as there is more to write
        }

        Ok(())
    }
}

fn main_recv(
    addr: SocketAddr,
    n_connections: u32,
    write_mode: WriteMode,
    protocol_version: u16,
    verbosity: Verbosity,
) -> std::result::Result<(), FastsyncError> {
    // First we initiate one connection. The sender will send the plan over
    // that. We read it. Unbuffered, because we want to skip the buffer for the
    // remaining reads, but the header is tiny so it should be okay.
    let mut stream = TcpStream::connect(addr)?;
    let plan = TransferPlan::deserialize_reader(&mut stream)?;
    if plan.proto_version != protocol_version {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!(
                "Sender is version {} and we only support {WIRE_PROTO_VERSION}",
                plan.proto_version
            ),
        )
        .into());
    }
    if write_mode == WriteMode::AskConfirm {
        plan.ask_confirm_receive()?;
    }

    // The pull threads are going to receive chunks and push them into this
    // channel. Then we have one IO writer thread that either parks the chunks
    // or writes them to disk. A small channel is enough for this: if the disk
    // is faster than the network then the channel will be empty most of the
    // time, and if the network is faster the channel will be full all the time.
    let (sender, receiver) = mpsc::sync_channel::<Chunk>(16);

    let writer_thread = std::thread::spawn(move || {
        let total_len: u64 = plan.files.iter().map(|f| f.len).sum();
        let mut files: Vec<_> = plan.files.into_iter().map(FileReceiver::new).collect();

        let start_time = Instant::now();
        let mut bytes_received: u64 = 0;

        for chunk in receiver {
            let file = &mut files[chunk.file_id.0 as usize];
            bytes_received += chunk.data.len() as u64;
            // On error, rather than exiting the thread and crashing the writing
            // end of the channel, just crash the entire program so that the
            // error message is clearer.
            file.handle_chunk(chunk).expect("Failed to write chunk.");
            let _ = print_progress(bytes_received, total_len, start_time);
        }

        if bytes_received < total_len {
            return Err(FastsyncError::AppError(
                "Transmission ended, but not all data was received.".to_string(),
            ));
        }
        Ok(())
    });

    // We make n threads that "pull" the data from a socket. The first socket we
    // already have, the transfer plan was sent on that one.
    let mut streams = vec![stream];
    for _ in 1..n_connections {
        match TcpStream::connect(addr) {
            // The sender stops listening after all transfers are complete. For
            // small transfers, it might have already sent the entire file on
            // the initial connection before we get a chance to open the others,
            // so connection refused is not a problem.
            Ok(stream) => streams.push(stream),
            Err(err) if err.kind() == ErrorKind::ConnectionRefused => break,
            Err(err) => panic!("Failed to connect to sender: {err:?}"),
        }
    }

    let mut pull_threads = Vec::new();
    for mut stream in streams {
        let sender_i = sender.clone();
        let thread_pull = std::thread::spawn::<_, Result<()>>(move || {
            loop {
                // Read a chunk header. If we hit EOF, that is not an error, it
                // means that the sender has nothing more to send so we can just
                // exit here.
                let mut buf = [0u8; 16];
                match stream.read_exact(&mut buf) {
                    Ok(..) => {}
                    Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                    Err(err) if err.kind() == ErrorKind::ConnectionReset => break,
                    Err(err) => return Err(err),
                };

                let header = ChunkHeader::try_from_slice(&buf[..])?;
                if Verbosity::Verbose == verbosity {
                    println!("RECV-CHUNK {:?}", header);
                }
                assert!(
                    (header.len as u64) <= MAX_CHUNK_LEN,
                    "{} <= {}",
                    header.len,
                    MAX_CHUNK_LEN
                );

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
        pull_thread.join().map_err(|err| {
            FastsyncError::AppError(format!("failed to join pull thread: {:?}", err))
        })??;
    }

    // After all pulls are done and the transfer is complete, the sender is
    // still stuck in its accept() call listening for potential additional
    // readers. One way to get around that is by doing a non-blocking accept,
    // but then we either have to busy-wait, or if we add a sleep then we create
    // a polling delay. Another way is to make everything async, but then we
    // have to add a dependency on the async ecosystem and pull in 100s of
    // crates, and create gigabytes of build artifacts, just to do a clean exit.
    // So as a hack, just connect one more time to wake up the sender's accept()
    // loop. It will conclude there is nothing to send and then exit.
    match TcpStream::connect(addr) {
        Ok(stream) => std::mem::drop(stream),
        // Too bad if we can't wake up the sender, but it's not our problem.
        Err(_) => {}
    }

    writer_thread.join().map_err(|err| {
        FastsyncError::AppError(format!("failed to join writer thread: {:?}", err))
    })??;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::{
        net::{IpAddr, Ipv4Addr},
        thread,
    };
    use tempfile::TempDir;

    #[test]
    fn test_accepts_valid_protocol() {
        let (events_tx, events_rx) = std::sync::mpsc::channel::<SenderEvent>();
        thread::spawn(|| {
            std::fs::File::create("a-file").unwrap();
            main_send(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0),
                &["a-file".into()],
                1,
                events_tx,
                None,
                Verbosity::Silent,
            )
            .unwrap();
        });
        match events_rx.recv().unwrap() {
            SenderEvent::Listening(port) => {
                main_recv(
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
                    1,
                    WriteMode::Force,
                    1,
                    Verbosity::Silent,
                )
                .unwrap();
            }
        }
    }

    #[test]
    fn test_refuses_invalid_protocol() {
        let (events_tx, events_rx) = std::sync::mpsc::channel::<SenderEvent>();
        thread::spawn(|| {
            std::fs::File::create("a-file").unwrap();
            main_send(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0),
                &["a-file".into()],
                2,
                events_tx,
                None,
                Verbosity::Silent,
            )
            .unwrap();
        });
        match events_rx.recv().unwrap() {
            SenderEvent::Listening(port) => {
                let res = main_recv(
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
                    1,
                    WriteMode::Force,
                    1,
                    Verbosity::Silent,
                );
                match res {
                    Ok(_) => panic!("Expected failure, but got success."),
                    Err(FastsyncError::IoError(err)) => {
                        assert_eq!(err.kind(), ErrorKind::InvalidData);
                    }
                    Err(err) => panic!("Expected IoError, but got {err:?}"),
                }
            }
        }
    }

    #[test]
    fn test_walk_paths_recursively() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let base_path = temp_dir.path();
        std::fs::create_dir_all(base_path.join("a/b")).unwrap();
        File::create(base_path.join("0")).unwrap();
        File::create(base_path.join("a/1")).unwrap();
        File::create(base_path.join("a/b/2")).unwrap();

        let mut res =
            all_filenames_from_path_names(&[base_path.to_str().unwrap().to_owned()]).unwrap();
        res.sort();

        assert_eq!(
            res,
            ["0", "a/1", "a/b/2"].map(|f| base_path.join(f).to_str().unwrap().to_owned())
        );
    }

    #[test]
    fn test_sends_large_file() {
        let (events_tx, events_rx) = std::sync::mpsc::channel::<SenderEvent>();
        env::set_current_dir("/tmp/").unwrap();
        let cwd = env::current_dir().unwrap();
        thread::spawn(|| {
            let td = TempDir::new_in(".").unwrap();
            let tmp_path = td.path().strip_prefix(cwd).unwrap();
            let path = tmp_path.join("large");
            let fnames = &[path.clone().into_os_string().into_string().unwrap()];

            {
                let mut f = std::fs::File::create(path).unwrap();
                f.write_all(&vec![0u8; MAX_CHUNK_LEN as usize * 100])
                    .unwrap();
            }

            main_send(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0),
                fnames,
                1,
                events_tx,
                None,
                Verbosity::Silent,
            )
            .unwrap();
        });
        match events_rx.recv().unwrap() {
            SenderEvent::Listening(port) => {
                main_recv(
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
                    1,
                    WriteMode::Force,
                    1,
                    Verbosity::Silent,
                )
                .unwrap();
            }
        }
    }
    #[test]
    fn test_sends_20_thousand_files() {
        let (events_tx, events_rx) = std::sync::mpsc::channel::<SenderEvent>();
        env::set_current_dir("/tmp/").unwrap();
        let cwd = env::current_dir().unwrap();
        thread::spawn(|| {
            let td = TempDir::new_in(".").unwrap();
            let tmp_path = td.path().strip_prefix(cwd).unwrap();
            let mut fnames = Vec::new();
            for i in 0..20_000 {
                let path = tmp_path.join(i.to_string());
                fnames.push(path.clone().into_os_string().into_string().unwrap());
                let mut f = std::fs::File::create(path).unwrap();
                f.write(&[1, 2, 3]).unwrap();
            }
            main_send(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0),
                &fnames,
                1,
                events_tx,
                None,
                Verbosity::Silent,
            )
            .unwrap();
        });
        match events_rx.recv().unwrap() {
            SenderEvent::Listening(port) => {
                main_recv(
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
                    1,
                    WriteMode::Force,
                    1,
                    Verbosity::Silent,
                )
                .unwrap();
            }
        }
    }

    #[test]
    fn file_deleted_before_send() {
        let (events_tx, events_rx) = std::sync::mpsc::channel::<SenderEvent>();
        env::set_current_dir("/tmp/").unwrap();
        let cwd = env::current_dir().unwrap();

        let td = TempDir::new_in(".").unwrap();
        let tmp_path = td.path().strip_prefix(cwd).unwrap();
        let path1 = tmp_path.join("file1");
        let path2 = tmp_path.join("file_deleted_before_send");
        let fnames = vec![
            path1.clone().into_os_string().into_string().unwrap(),
            path2.clone().into_os_string().into_string().unwrap(),
        ];

        let sender_handle = thread::spawn(move || {
            {
                for path in &fnames {
                    let mut f = std::fs::File::create(path).unwrap();
                    f.write_all(&vec![0u8; MAX_CHUNK_LEN as usize * 3]).unwrap();
                }
            }

            let res = main_send(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0),
                &fnames,
                1,
                events_tx,
                None,
                Verbosity::Silent,
            );
            res
        });

        match events_rx.recv().unwrap() {
            SenderEvent::Listening(port) => {
                // Remove file after the sender has listed it
                std::fs::remove_file(path2).expect("Failed to delete file before send");

                main_recv(
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
                    1,
                    WriteMode::Force,
                    1,
                    Verbosity::Silent,
                )
                .expect_err("expected receiver to fail");
            }
        }

        let result = sender_handle.join().expect("Failed to join sender thread.");
        match result {
            Ok(_) => panic!("Expected failure, but got success."),
            Err(FastsyncError::AppError(err)) => {
                assert!(err.contains("vanished during transfer"));
            }
            Err(err) => panic!("Expected AppError, but got {err:?}"),
        }
    }
}
