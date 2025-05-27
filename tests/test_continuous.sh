#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
FASTSYNC_BIN="$PROJECT_ROOT/target/release/fastsync"

# Build the project if binary doesn't exist
if [ ! -f "$FASTSYNC_BIN" ]; then
    echo "Building fastsync..."
    cd "$PROJECT_ROOT"
    cargo build --release
fi

echo -e "${YELLOW}=== Testing Continuous Mode ===${NC}"

# Clean up any previous test directories
rm -rf /tmp/fastsync_test_src /tmp/fastsync_test_dst
mkdir -p /tmp/fastsync_test_src /tmp/fastsync_test_dst

# Create initial test files
echo "Creating initial test files..."
echo "File 1 content" > /tmp/fastsync_test_src/file1.txt
echo "File 2 content" > /tmp/fastsync_test_src/file2.txt
echo "File 3 content" > /tmp/fastsync_test_src/file3.txt

# Start sender in continuous mode (using . to monitor current directory)
echo -e "\n${YELLOW}Starting sender in continuous mode...${NC}"
cd /tmp/fastsync_test_src
"$FASTSYNC_BIN" send 127.0.0.1:8899 --continuous . 2>&1 | tee /tmp/fastsync_sender.log &
SENDER_PID=$!
echo "Sender PID: $SENDER_PID"
sleep 2

# Start receiver in continuous mode
echo -e "\n${YELLOW}Starting receiver in continuous mode...${NC}"
cd /tmp/fastsync_test_dst
echo "y" | "$FASTSYNC_BIN" recv 127.0.0.1:8899 2 --continuous 2>&1 | tee /tmp/fastsync_receiver.log &
RECEIVER_PID=$!
echo "Receiver PID: $RECEIVER_PID"

# Wait for initial sync
echo -e "\n${YELLOW}Waiting for initial sync...${NC}"
sleep 3

# Check initial sync
echo -e "\n${YELLOW}Checking initial sync...${NC}"
if [ -f /tmp/fastsync_test_dst/file1.txt ] && \
   [ -f /tmp/fastsync_test_dst/file2.txt ] && \
   [ -f /tmp/fastsync_test_dst/file3.txt ]; then
    echo -e "${GREEN}✓ Initial sync successful${NC}"
else
    echo -e "${RED}✗ Initial sync failed${NC}"
    kill $SENDER_PID $RECEIVER_PID 2>/dev/null
    exit 1
fi

# Test 1: Modify a file
echo -e "\n${YELLOW}Test 1: Modifying file2.txt...${NC}"
echo "File 2 modified content" > /tmp/fastsync_test_src/file2.txt
touch /tmp/fastsync_test_src/file2.txt
sleep 3

if [ "$(cat /tmp/fastsync_test_dst/file2.txt)" = "File 2 modified content" ]; then
    echo -e "${GREEN}✓ File modification synced${NC}"
else
    echo -e "${RED}✗ File modification not synced${NC}"
    echo "Expected: 'File 2 modified content'"
    echo "Got: '$(cat /tmp/fastsync_test_dst/file2.txt)'"
fi

# Test 2: Add a new file
echo -e "\n${YELLOW}Test 2: Adding file4.txt...${NC}"
echo "File 4 new content" > /tmp/fastsync_test_src/file4.txt
sleep 3

if [ -f /tmp/fastsync_test_dst/file4.txt ] && \
   [ "$(cat /tmp/fastsync_test_dst/file4.txt)" = "File 4 new content" ]; then
    echo -e "${GREEN}✓ New file synced${NC}"
else
    echo -e "${RED}✗ New file not synced${NC}"
fi

# Test 3: Delete a file (should not be deleted on receiver)
echo -e "\n${YELLOW}Test 3: Deleting file1.txt from sender...${NC}"
rm /tmp/fastsync_test_src/file1.txt
sleep 3

if [ -f /tmp/fastsync_test_dst/file1.txt ]; then
    echo -e "${GREEN}✓ File correctly retained on receiver${NC}"
else
    echo -e "${RED}✗ File incorrectly deleted on receiver${NC}"
fi

# Test 4: Modify multiple files
echo -e "\n${YELLOW}Test 4: Modifying multiple files...${NC}"
echo "File 3 modified content" > /tmp/fastsync_test_src/file3.txt
echo "File 4 modified content" > /tmp/fastsync_test_src/file4.txt
sleep 3

if [ "$(cat /tmp/fastsync_test_dst/file3.txt)" = "File 3 modified content" ] && \
   [ "$(cat /tmp/fastsync_test_dst/file4.txt)" = "File 4 modified content" ]; then
    echo -e "${GREEN}✓ Multiple file modifications synced${NC}"
else
    echo -e "${RED}✗ Multiple file modifications not synced${NC}"
fi

# Check logs for expected behavior
echo -e "\n${YELLOW}Checking sender logs...${NC}"
if grep -q "is up to date at receiver" /tmp/fastsync_sender.log; then
    echo -e "${GREEN}✓ Sender correctly identifies up-to-date files${NC}"
fi
if grep -q "sending complete file" /tmp/fastsync_sender.log; then
    echo -e "${GREEN}✓ Sender sends modified files${NC}"
fi

echo -e "\n${YELLOW}Checking receiver logs...${NC}"
if grep -q "Continuous sync round" /tmp/fastsync_receiver.log; then
    echo -e "${GREEN}✓ Receiver shows continuous rounds${NC}"
fi

# Kill processes
echo -e "\n${YELLOW}Stopping sender and receiver...${NC}"
kill $SENDER_PID $RECEIVER_PID 2>/dev/null

echo -e "\n${YELLOW}Test completed!${NC}"
echo "Sender log: /tmp/fastsync_sender.log"
echo "Receiver log: /tmp/fastsync_receiver.log"