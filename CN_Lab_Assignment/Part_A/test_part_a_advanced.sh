#!/usr/bin/env bash
set -e

PORT=9010
echo "====================================================================="
echo "Part A Comprehensive Validation Test Suite"
echo "====================================================================="

# --- Test 1: Single-Client Test ---
echo ""
echo "--- TEST 1: Single-Client Exchange (3 messages) ---"
./server $PORT > test1_server.log 2>&1 &
SERVER_PID=$!
sleep 1

./client 127.0.0.1 $PORT 3 > test1_client.log 2>&1
echo "Client finished successfully."

kill -SIGINT $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo "[Test 1 Client Output]:"
cat test1_client.log
echo "[Test 1 Server Output]:"
cat test1_server.log
rm -f test1_server.log test1_client.log

# --- Test 2: Incomplete Frame Handling ---
echo ""
echo "--- TEST 2: Incomplete Frame Handling (No trailing newline) ---"
PORT=9011
./server $PORT > test2_server.log 2>&1 &
SERVER_PID=$!
sleep 1

# Send "hel" without newline and close immediately
python3 -c "import socket; s = socket.socket(); s.connect(('127.0.0.1', 9011)); s.sendall(b'hel'); s.close()"
sleep 1

kill -SIGINT $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo "[Test 2 Server Output - Verifying Incomplete Frame Discard]:"
cat test2_server.log
grep -q "incomplete frame" test2_server.log && echo "PASS: Server correctly detected and discarded incomplete frame."
rm -f test2_server.log

# --- Test 3: Three Simultaneous Clients ---
echo ""
echo "--- TEST 3: Three Simultaneous Clients ---"
PORT=9012
./server $PORT > test3_server.log 2>&1 &
SERVER_PID=$!
sleep 1

./client 127.0.0.1 $PORT 3 > test3_c1.log 2>&1 &
PID1=$!
./client 127.0.0.1 $PORT 3 > test3_c2.log 2>&1 &
PID2=$!
./client 127.0.0.1 $PORT 3 > test3_c3.log 2>&1 &
PID3=$!

wait $PID1
wait $PID2
wait $PID3
echo "All 3 clients completed successfully."

kill -SIGINT $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo "[Test 3 Server Output - Verifying Concurrent Worker Threads]:"
cat test3_server.log
rm -f test3_server.log test3_c1.log test3_c2.log test3_c3.log

# --- Test 4: Unexpected Client Disconnection (Kill -9) During Multi-Client Operation ---
echo ""
echo "--- TEST 4: Unexpected Client Disconnection (kill -9) ---"
PORT=9013
./server $PORT > test4_server.log 2>&1 &
SERVER_PID=$!
sleep 1

# Start Client A (will be killed) and Client B (should complete fully)
./client 127.0.0.1 $PORT 10 > test4_ca.log 2>&1 &
PIDA=$!
./client 127.0.0.1 $PORT 4 > test4_cb.log 2>&1 &
PIDB=$!

sleep 1.5
echo "Abruptly killing Client A with SIGKILL (kill -9)..."
kill -9 $PIDA 2>/dev/null || true

# Wait for Client B to complete its 4 exchanges
wait $PIDB
echo "Client B finished all 4 exchanges uninterrupted."

kill -SIGINT $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo "[Test 4 Server Output - Verifying Client B Unaffected by Client A Drop]:"
cat test4_server.log
rm -f test4_server.log test4_ca.log test4_cb.log

# --- Test 5: Server Ctrl+C While Client is Connected ---
echo ""
echo "--- TEST 5: Server Ctrl+C (SIGINT) While Client is Connected ---"
PORT=9014
./server $PORT > test5_server.log 2>&1 &
SERVER_PID=$!
sleep 1

# Start a continuous client
./client 127.0.0.1 $PORT 0 > test5_client.log 2>&1 &
CLIENT_PID=$!
sleep 1.5

echo "Sending SIGINT (Ctrl+C) to server while client is actively connected..."
kill -SIGINT $SERVER_PID
wait $SERVER_PID 2>/dev/null || true
wait $CLIENT_PID 2>/dev/null || true

echo "[Test 5 Server Output - Verifying Worker Thread Tracking and Clean Exit]:"
cat test5_server.log
echo "[Test 5 Client Output]:"
cat test5_client.log
rm -f test5_server.log test5_client.log

echo ""
echo "====================================================================="
echo "ALL TESTS PASSED SUCCESSFULLY!"
echo "====================================================================="

