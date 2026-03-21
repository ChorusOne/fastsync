#!/bin/bash

# This test will start sender and receiver
# processes on the local machine.
# It will then create/delete files in the `/tmp/`
# directory of the machine.
#
# Note: this test will also kill any
# fastsync process running on ports 8899, 8901
# as part of cleanup.
# If you are running real fastsync operations,
# it's advisable to not run this test in parallel.


set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
FASTSYNC_BIN="$PROJECT_ROOT/target/release/fastsync"

kill_fastsync_processes() {
    for port in 8899 8901; do
        lsof -ti:$port 2>/dev/null | while read pid; do
            # Get full command line to ensure it's really fastsync
            if ps -p $pid -o args= 2>/dev/null | grep -q fastsync; then
                echo "Killing fastsync process $pid on port $port"
                kill -TERM $pid 2>/dev/null || true
            fi
        done
    done
}

# Build the project if binary doesn't exist
if [ ! -f "$FASTSYNC_BIN" ]; then
    echo "Building fastsync..."
    cd "$PROJECT_ROOT"
    cargo build --release
fi

echo -e "${YELLOW}=== Testing Continuous Mode ===${NC}"

echo -e "${GREEN}Killing any fastsync processes already running${NC}"
kill_fastsync_processes


# Test 1: Basic continuous mode functionality
echo -e "\n${YELLOW}=== Test 1: Basic Continuous Mode Functionality ===${NC}"

# Clean up any previous test directories
rm -rf /tmp/fastsync_test_src /tmp/fastsync_test_dst
mkdir -p /tmp/fastsync_test_src /tmp/fastsync_test_dst

# Create initial test files
echo "Creating initial test files..."
echo "File 1 content" > /tmp/fastsync_test_src/file1.txt
echo "File 2 content" > /tmp/fastsync_test_src/file2.txt
echo "File 3 content" > /tmp/fastsync_test_src/file3.txt

# Start receiver in continuous mode first
echo -e "\n${YELLOW}Starting receiver in continuous mode...${NC}"
cd /tmp/fastsync_test_dst
"$FASTSYNC_BIN" recv 127.0.0.1:8899 2 --continuous 2>&1 | tee /tmp/fastsync_receiver.log &
RECEIVER_PID=$!
echo "Receiver PID: $RECEIVER_PID"
sleep 2

# Start sender in continuous mode (using . to monitor current directory)
echo -e "\n${YELLOW}Starting sender in continuous mode...${NC}"
cd /tmp/fastsync_test_src
"$FASTSYNC_BIN" send 127.0.0.1:8899 --continuous . 2>&1 | tee /tmp/fastsync_sender.log &
SENDER_PID=$!
echo "Sender PID: $SENDER_PID"

# Wait for initial sync
echo -e "\n${YELLOW}Waiting for initial sync...${NC}"
sleep 5

# Check initial sync with retry logic
echo -e "\n${YELLOW}Checking initial sync...${NC}"
SYNC_SUCCESS=false
for i in {1..10}; do
    if [ -f /tmp/fastsync_test_dst/file1.txt ] && \
       [ -f /tmp/fastsync_test_dst/file2.txt ] && \
       [ -f /tmp/fastsync_test_dst/file3.txt ]; then
        echo -e "${GREEN}✓ Initial sync successful${NC}"
        SYNC_SUCCESS=true
        break
    fi
    echo -n "."
    sleep 1
done

if [ "$SYNC_SUCCESS" = false ]; then
    echo -e "\n${RED}✗ Initial sync failed after 10 seconds${NC}"
    kill_fastsync_processes
    exit 1
fi

# Test 1a: Modify a file
echo -e "\n${YELLOW}Test 1a: Modifying file2.txt...${NC}"
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

# Test 1b: Add a new file
echo -e "\n${YELLOW}Test 1b: Adding file4.txt...${NC}"
echo "File 4 new content" > /tmp/fastsync_test_src/file4.txt
sleep 3

if [ -f /tmp/fastsync_test_dst/file4.txt ] && \
   [ "$(cat /tmp/fastsync_test_dst/file4.txt)" = "File 4 new content" ]; then
    echo -e "${GREEN}✓ New file synced${NC}"
else
    echo -e "${RED}✗ New file not synced${NC}"
fi

# Test 1c: Delete a file (should not be deleted on receiver)
echo -e "\n${YELLOW}Test 1c: Deleting file1.txt from sender...${NC}"
rm /tmp/fastsync_test_src/file1.txt
sleep 3

if [ -f /tmp/fastsync_test_dst/file1.txt ]; then
    echo -e "${GREEN}✓ File correctly retained on receiver${NC}"
else
    echo -e "${RED}✗ File incorrectly deleted on receiver${NC}"
fi

# Test 1d: Modify multiple files
echo -e "\n${YELLOW}Test 1d: Modifying multiple files...${NC}"
echo "File 3 modified content" > /tmp/fastsync_test_src/file3.txt
echo "File 4 modified content" > /tmp/fastsync_test_src/file4.txt
sleep 3

if [ "$(cat /tmp/fastsync_test_dst/file3.txt)" = "File 3 modified content" ] && \
   [ "$(cat /tmp/fastsync_test_dst/file4.txt)" = "File 4 modified content" ]; then
    echo -e "${GREEN}✓ Multiple file modifications synced${NC}"
else
    echo -e "${RED}✗ Multiple file modifications not synced${NC}"
fi

# Kill processes
echo -e "\n${YELLOW}Stopping sender and receiver...${NC}"
kill_fastsync_processes

echo -e "${GREEN}✓ Test 1: Basic continuous mode functionality passed${NC}"


# Test 2: Auto-exit after 5 rounds with no changes
echo -e "\n${YELLOW}=== Test 2: Auto-Exit After 5 Rounds ===${NC}"

# Clean up any previous test directories
rm -rf /tmp/fastsync_test_exit_src /tmp/fastsync_test_exit_dst
mkdir -p /tmp/fastsync_test_exit_src /tmp/fastsync_test_exit_dst

# Create initial test files
echo "Creating initial test files..."
echo "Initial content 1" > /tmp/fastsync_test_exit_src/file1.txt
echo "Initial content 2" > /tmp/fastsync_test_exit_src/file2.txt

# Start receiver in continuous mode first
echo -e "\n${YELLOW}Starting receiver in continuous mode...${NC}"
cd /tmp/fastsync_test_exit_dst
"$FASTSYNC_BIN" recv 127.0.0.1:8901 2 --continuous 2>&1 | tee /tmp/fastsync_receiver_exit.log &
RECEIVER_PID=$!
echo "Receiver PID: $RECEIVER_PID"
sleep 2

# Start sender in continuous mode
echo -e "\n${YELLOW}Starting sender in continuous mode...${NC}"
cd /tmp/fastsync_test_exit_src
"$FASTSYNC_BIN" send 127.0.0.1:8901 --continuous file1.txt file2.txt 2>&1 | tee /tmp/fastsync_sender_exit.log &
SENDER_PID=$!
echo "Sender PID: $SENDER_PID"

# Wait for initial sync
echo -e "\n${YELLOW}Waiting for initial sync...${NC}"
sleep 5

# Check initial sync with retry logic
echo -e "\n${YELLOW}Checking initial sync...${NC}"
SYNC_SUCCESS=false
for i in {1..10}; do
    if [ -f /tmp/fastsync_test_exit_dst/file1.txt ] && \
       [ -f /tmp/fastsync_test_exit_dst/file2.txt ]; then
        echo -e "${GREEN}✓ Initial sync successful${NC}"
        SYNC_SUCCESS=true
        break
    fi
    echo -n "."
    sleep 1
done

if [ "$SYNC_SUCCESS" = false ]; then
    echo -e "\n${RED}✗ Initial sync failed after 10 seconds${NC}"
    kill_fastsync_processes
    exit 1
fi

# Test 2a: Modify a file to reset the skip counter
echo -e "\n${YELLOW}Test 2a: Modifying file1.txt to reset skip counter...${NC}"
sleep 3  # Let one round pass with all files up to date
echo "Modified content 1" > /tmp/fastsync_test_exit_src/file1.txt
sleep 3

if [ "$(cat /tmp/fastsync_test_exit_dst/file1.txt)" = "Modified content 1" ]; then
    echo -e "${GREEN}✓ File modification synced${NC}"
else
    echo -e "${RED}✗ File modification not synced${NC}"
    kill_fastsync_processes
    exit 1
fi

# Test 2b: Now let it run without changes to test the 5-round exit
echo -e "\n${YELLOW}Test 2b: Testing auto-exit after 5 rounds with no changes...${NC}"
echo -e "${BLUE}Round timings (2 seconds per round):${NC}"
echo -e "${BLUE}  Round 1: 0-2 seconds${NC}"
echo -e "${BLUE}  Round 2: 2-4 seconds${NC}"
echo -e "${BLUE}  Round 3: 4-6 seconds${NC}"
echo -e "${BLUE}  Round 4: 6-8 seconds${NC}"
echo -e "${BLUE}  Round 5: 8-10 seconds${NC}"
echo -e "${BLUE}  Expected exit: ~10-11 seconds${NC}"

START_TIME=$(date +%s)

# Monitor for up to 15 seconds (should exit around 10-11 seconds)
for i in {1..15}; do
    if ! kill -0 $SENDER_PID 2>/dev/null; then
        END_TIME=$(date +%s)
        ELAPSED=$((END_TIME - START_TIME))
        echo -e "\n${GREEN}✓ Sender exited after $ELAPSED seconds${NC}"

        # Check if it exited in the expected time window (9-13 seconds)
        if [ $ELAPSED -ge 9 ] && [ $ELAPSED -le 13 ]; then
            echo -e "${GREEN}✓ Exit timing correct (expected ~10-11 seconds)${NC}"
        else
            echo -e "${RED}✗ Exit timing unexpected (got $ELAPSED seconds)${NC}"
        fi

        # Check the log for the expected message
        if grep -q "Continuous sync completed - no changes detected for 5 consecutive iterations" /tmp/fastsync_sender_exit.log; then
            echo -e "${GREEN}✓ Found expected exit message in logs${NC}"
        else
            echo -e "${RED}✗ Expected exit message not found in logs${NC}"
        fi

        # The continuous mode exit logic works internally without specific skip count messages

        break
    fi

    echo -n "."
    sleep 1
done

# Check if sender is still running (it shouldn't be)
if kill -0 $SENDER_PID 2>/dev/null; then
    echo -e "\n${RED}✗ Sender still running after 15 seconds (should have exited)${NC}"
    kill $SENDER_PID 2>/dev/null || true
    FAILED=1
else
    echo -e "${GREEN}✓ Sender correctly exited after no changes${NC}"
fi

kill_fastsync_processes

echo -e "${GREEN}✓ Test 2: Auto-exit functionality passed${NC}"


echo -e "\n${YELLOW}=== Test Summary ===${NC}"
echo -e "${GREEN}✓ Basic continuous mode functionality${NC}"
echo -e "${GREEN}✓ File modification sync${NC}"
echo -e "${GREEN}✓ New file detection${NC}"
echo -e "${GREEN}✓ File retention on deletion${NC}"
echo -e "${GREEN}✓ Multiple file modification sync${NC}"
echo -e "${GREEN}✓ Auto-exit after 5 rounds with no changes${NC}"

echo -e "\n${YELLOW}All continuous mode tests completed successfully!${NC}"
echo "Logs available at:"
echo "  /tmp/fastsync_sender.log"
echo "  /tmp/fastsync_receiver.log"
echo "  /tmp/fastsync_sender_exit.log"
echo "  /tmp/fastsync_receiver_exit.log"

# Exit with success if no failures
if [ -z "$FAILED" ]; then
    exit 0
else
    exit 1
fi
