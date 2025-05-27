# Fastsync

Fastsync transfers files and/or directories between machines as fast as the
network allows. Tools that transfer files over a single TCP connection — what
`rsync`, `scp`, and even raw `netcat` do — often fail to saturate the network
link. This is due to [head-of-line blocking][tcp-hol]. Opening multiple TCP
connections can bring a significant boost in transfer speed.

Fastsync targets the following use case:

 * **Linux only.** For now.
 * **Confidentiality and authentication are handled externally.** Fastsync does
   not encrypt the files or authenticate the receiver. It assumes you are using
   it over e.g. a Wireguard network interface.
 * **Compression is handled externally.** Fastsync does not compress the stream.
   If the data volume benefits from compression, then compress the files ahead
   of time with e.g. `lz4`, `brotli`, or `zstd`.

## Building

For your local machine:

    cargo build --release
    target/release/fastsync

Build a static binary that is more likely to be portable:

    cargo build --release --target x86_64-unknown-linux-musl
    ldd target/x86_64-unknown-linux-musl/release/fastsync

## How to use

Suppose the sender has Tailscale IP 100.71.154.83. Pick some available port,
like 4440 (assuming it's not bound to). Then on the sending end:

    fastsync send 100.71.154.83:4440 file.tar.gz

Alternatively if you want to send an entire directory, then you should something
like the command below. Please note that fastsync will not allow absolute paths:

    cd /some/path/
    fastsync send 100.71.154.83:4440 ./data

On the receiving end, suppose we download with 32 TCP connections:

    cd /some/path
    fastsync recv 100.71.154.83:4440 32

File modification timestamps are preserved during all transfers.

## Continuous mode

Fastsync supports continuous mode with the `--continuous` flag. When enabled, fastsync will:

1. Compare files by name, size, and modification timestamp
2. Skip files that already exist at the destination with matching size and timestamp
3. Transfer only files that are missing or have different size/timestamp
4. Keep syncing in rounds until no changes are detected
5. After each transfer round, check if any files were modified during the transfer
6. If changes are detected, start another sync round
7. Stop when a complete round finishes with no changes detected

Both sender and receiver must use the `--continuous` flag:

    # Sender
    fastsync send 100.71.154.83:4440 --continuous ./data

    # Receiver
    fastsync recv 100.71.154.83:4440 32 --continuous

This mode is particularly useful for:
- Syncing directories where files are still being written
- Live database migrations where you sync while the database is running
- Backup scenarios where files are being actively modified
- Any situation where you need to minimize downtime during a transfer

Typical workflow with `--continuous`:
1. Start continuous mode while the source system is live
2. Monitor the output - transfers will become smaller each round
3. When transfers are minimal, stop the source application
4. Let fastsync complete the final round
5. You now have a complete, consistent copy

## Testing

To run all tests:

    cargo test

To run only unit tests:

    cargo test --lib

To run integration tests:

    cd tests && ./integration_tests.sh

## Known issues

 * It's too spammy.
 * Transfer time estimation can be improved.

## License

Fastsync is licensed under the [Apache 2.0 License][apache2]. A copy of the
license is included in the root of the repository.

[apache2]: https://www.apache.org/licenses/LICENSE-2.0
[tcp-hol]: https://github.com/rmarx/holblocking-blogpost/blob/a128994e32c134c5af6eb30120e321806cd6a4a3/README.md#tcp-hol-blocking
