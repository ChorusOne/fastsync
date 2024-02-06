# Fastsync

For when you need to transfer files between machines fast, but somehow it's not
saturating the network card. This tool transfers over multiple TCP connections
to try and saturate it.

## How to use

Building:

    cargo build --release
    target/release/fastsync

Suppose the sender has Tailscale IP 100.71.154.83. Pick some available port,
like 7999 (assuming it's not bound to). Then on the sending end:

    fastsync send 100.71.154.83:7999 file.tar.gz

On the receiving end, suppose we download with 32 TCP connections:

    fastsync recv 100.71.154.83:7999 file.tar.gz 32

## Known issues

 * The sender doesn't exit when it's done.
 * It's too spammy.
