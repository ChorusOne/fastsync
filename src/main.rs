use std::io::Result;

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

fn main_send(addr: &str, fname: &str) -> Result<()> {
    eprintln!("TODO: Send {addr} {fname}.");
    Ok(())
}

fn main_recv(addr: &str, fname: &str) -> Result<()> {
    eprintln!("TODO: Recv {addr} {fname}.");
    Ok(())
}
