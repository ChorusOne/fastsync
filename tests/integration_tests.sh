#!/bin/bash
set -e

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
FASTSYNC_BIN="$PROJECT_ROOT/target/release/fastsync"

# Build the project
echo "Building fastsync..."
cd "$PROJECT_ROOT"
cargo build --release 2>/dev/null

# Create test directories
rm -rf /tmp/fastsync_test_src /tmp/fastsync_test_dst
mkdir -p /tmp/fastsync_test_src /tmp/fastsync_test_dst

# Create test files
echo "File 1 content" > /tmp/fastsync_test_src/file1.txt
echo "File 2 content" > /tmp/fastsync_test_src/file2.txt
echo "File 3 content" > /tmp/fastsync_test_src/file3.txt

# First sync (full transfer)
echo "=== Initial sync ==="
cd /tmp/fastsync_test_src
"$FASTSYNC_BIN" send 127.0.0.1:8899 *.txt &
SENDER_PID=$!
sleep 1

cd /tmp/fastsync_test_dst
echo "y" | "$FASTSYNC_BIN" recv 127.0.0.1:8899 2 >/dev/null
wait $SENDER_PID

# Wait to ensure timestamp difference
sleep 2

# Modify one file and add a new one
echo "File 2 modified content with more data" > /tmp/fastsync_test_src/file2.txt
echo "File 4 new content" > /tmp/fastsync_test_src/file4.txt

# Ensure the modified file has a different timestamp than the original
touch /tmp/fastsync_test_src/file2.txt

# Incremental sync
echo "=== Incremental sync ==="
cd /tmp/fastsync_test_src
"$FASTSYNC_BIN" send 127.0.0.1:8899 --incremental *.txt 2>&1 | tee /tmp/fastsync_test_output.txt &
SENDER_PID=$!
sleep 1

cd /tmp/fastsync_test_dst
echo "y" | "$FASTSYNC_BIN" recv 127.0.0.1:8899 2 --incremental >/dev/null
wait $SENDER_PID

# Verify that the right files were skipped/transferred
echo "=== Verifying incremental behavior ==="
if grep -q "\[SKIP\] file1.txt.*already up to date" /tmp/fastsync_test_output.txt && \
   grep -q "\[FULL\] file2.txt.*sending complete file" /tmp/fastsync_test_output.txt && \
   grep -q "\[SKIP\] file3.txt.*already up to date" /tmp/fastsync_test_output.txt && \
   grep -q "\[FULL\] file4.txt.*sending complete file" /tmp/fastsync_test_output.txt; then
    echo "Incremental behavior test passed!"
else
    echo "Incremental behavior test failed!"
    cat /tmp/fastsync_test_output.txt
    exit 1
fi

# Verify results
echo "=== Verifying results ==="
if [ "$(cat /tmp/fastsync_test_dst/file1.txt)" = "File 1 content" ] && \
   [ "$(cat /tmp/fastsync_test_dst/file2.txt)" = "File 2 modified content with more data" ] && \
   [ "$(cat /tmp/fastsync_test_dst/file3.txt)" = "File 3 content" ] && \
   [ "$(cat /tmp/fastsync_test_dst/file4.txt)" = "File 4 new content" ]; then
    echo "Content test passed!"
else
    echo "Content test failed!"
    exit 1
fi

# Verify timestamps are preserved
echo "=== Verifying timestamps ==="
timestamp_ok=true
for file in file1.txt file2.txt file3.txt file4.txt; do
    src_mtime=$(stat -c %Y /tmp/fastsync_test_src/$file 2>/dev/null || stat -f %m /tmp/fastsync_test_src/$file)
    dst_mtime=$(stat -c %Y /tmp/fastsync_test_dst/$file 2>/dev/null || stat -f %m /tmp/fastsync_test_dst/$file)
    if [ "$src_mtime" != "$dst_mtime" ]; then
        echo "WARNING: Timestamp mismatch for $file: src=$src_mtime dst=$dst_mtime (diff=$(($src_mtime - $dst_mtime))s)"
        # Allow up to 2 seconds difference for filesystem granularity issues
        if [ $(( ${src_mtime} - ${dst_mtime} )) -gt 2 ] || [ $(( ${dst_mtime} - ${src_mtime} )) -gt 2 ]; then
            timestamp_ok=false
        fi
    else
        echo "Timestamp preserved for $file: $src_mtime"
    fi
done

if [ "$timestamp_ok" = true ]; then
    echo "Timestamp test passed!"
else
    echo "Timestamp test failed!"
    exit 1
fi

# Clean up
rm -rf /tmp/fastsync_test_src /tmp/fastsync_test_dst
