# Fastsync

Fastsync transfers files between machines as fast as the network allows. Tools
that transfer files over a single TCP connection — what `rsync`, `scp`, and
even raw `netcat` do — often fail to saturate the network link. This is due to
[head-of-line blocking][tcp-hol]. Opening multiple TCP connections can bring
a significant boost in transfer speed.

Fastsync targets the following use case:

 * **Linux only.** For now.
 * **Confidentiality and authentication are handled externally.** Fastsync does
   not encrypt the files or authenticate the receiver. It assumes you are using
   it over e.g. a Wireguard network interface.
 * **Compression is handled externally.** Fastsync does not compress the stream.
   If the data volume benefits from compression, then compress the files ahead
   of time with e.g. `lz4`, `brotli`, or `zstd`.
 * **A full transfer is necessary.** Fastsync always sends all files. If some
   files are already present at the receiving side, or similar data is already
   present, `rsync` might be a better fit.

## Building

For your local machine:

    cargo build --release
    target/release/fastsync

Build a static binary that is more likely to be portable:

    cargo build --release --target x86_64-unknown-linux-musl
    ldd target/x86_64-unknown-linux-musl/release/fastsync

## How to use

Suppose the sender has Tailscale IP 100.71.154.83. Pick some available port,
like 7999 (assuming it's not bound to). Then on the sending end:

    fastsync send 100.71.154.83:7999 file.tar.gz

On the receiving end, suppose we download with 32 TCP connections:

    fastsync recv 100.71.154.83:7999 32

## Known issues

 * It's too spammy.
 * Transfer time estimation can be improved.

## License

Fastsync is licensed under the [Apache 2.0 License][apache2]. A copy of the
license is included in the root of the repository.

[apache2]: https://www.apache.org/licenses/LICENSE-2.0
[tcp-hol]: https://github.com/rmarx/holblocking-blogpost/blob/a128994e32c134c5af6eb30120e321806cd6a4a3/README.md#tcp-hol-blocking
