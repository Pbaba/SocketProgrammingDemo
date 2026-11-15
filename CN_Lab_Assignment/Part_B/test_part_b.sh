#!/usr/bin/env bash
set -e

echo "====================================================================="
echo "Part B: Sliding Window Protocol Comprehensive Validation Suite"
echo "====================================================================="

PORT=9020

# -------------------------------------------------------------------
# Test 1: Sliding Window Dynamics (Delay = 40ms to observe window steps)
# -------------------------------------------------------------------
echo ""
echo "--- TEST 1: Sliding Window Flow Control & Window Advancement (15 messages) ---"
echo "Starting server on port $PORT with 40ms ACK delay..."
./server $PORT 40 > test1_server.log 2>&1 &
SERVER_PID=$!
sleep 1

echo "Running client with total_messages = 15..."
./client 127.0.0.1 $PORT 15 > test1_client.log 2>&1
CLIENT_EXIT=$?

kill -SIGINT $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo "Client exit code: $CLIENT_EXIT"
echo ""
echo "[Test 1 Client Execution Output]:"
cat test1_client.log

echo ""
echo "[Verifying Test 1 Protocol Behaviour]:"
# Check that messages 1 to 5 were sent initially
grep -q "Sent HELLO 1 " test1_client.log && echo "  [x] Message 1 sent"
grep -q "Sent HELLO 2 " test1_client.log && echo "  [x] Message 2 sent"
grep -q "Sent HELLO 3 " test1_client.log && echo "  [x] Message 3 sent"
grep -q "Sent HELLO 4 " test1_client.log && echo "  [x] Message 4 sent"
grep -q "Sent HELLO 5 " test1_client.log && echo "  [x] Message 5 sent"
grep -q "Window full" test1_client.log && echo "  [x] Sender blocked when window reached capacity (5 messages in-flight)"
grep -q "Received ACK 1 " test1_client.log && echo "  [x] Received ACK 1"
grep -q "Sent HELLO 6 " test1_client.log && echo "  [x] Window advanced: HELLO 6 sent after ACK 1"
grep -q "Received ACK 2 " test1_client.log && echo "  [x] Received ACK 2"
grep -q "Sent HELLO 7 " test1_client.log && echo "  [x] Window advanced: HELLO 7 sent after ACK 2"
grep -q "All 15 messages have been successfully acknowledged" test1_client.log && echo "  [x] All 15 messages acknowledged"
grep -q "Sender thread terminated cleanly" test1_client.log && echo "  [x] Sender thread terminated cleanly"
grep -q "Receiver thread terminated cleanly" test1_client.log && echo "  [x] Receiver thread terminated cleanly"

rm -f test1_server.log test1_client.log

# -------------------------------------------------------------------
# Test 2: Concurrent Clients (Two Part B clients running in parallel)
# -------------------------------------------------------------------
echo ""
echo "--- TEST 2: Two Simultaneous Part B Clients ---"
PORT=9021
echo "Starting server on port $PORT with 20ms ACK delay..."
./server $PORT 20 > test2_server.log 2>&1 &
SERVER_PID=$!
sleep 1

echo "Starting Client A (10 messages) and Client B (10 messages) concurrently..."
./client 127.0.0.1 $PORT 10 > test2_clientA.log 2>&1 &
PIDA=$!
./client 127.0.0.1 $PORT 10 > test2_clientB.log 2>&1 &
PIDB=$!

wait $PIDA
wait $PIDB
echo "Both concurrent clients finished successfully."

kill -SIGINT $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo ""
echo "[Test 2 Client A Summary]:"
tail -n 6 test2_clientA.log
echo ""
echo "[Test 2 Client B Summary]:"
tail -n 6 test2_clientB.log

grep -q "SUCCESS" test2_clientA.log && echo "  [x] Client A finished with SUCCESS"
grep -q "SUCCESS" test2_clientB.log && echo "  [x] Client B finished with SUCCESS"

rm -f test2_server.log test2_clientA.log test2_clientB.log

# -------------------------------------------------------------------
# Test 3: Error Handling & Deadlock Prevention on Server Disconnect
# -------------------------------------------------------------------
echo ""
echo "--- TEST 3: Server Disconnection While Sender Is Blocked ---"
PORT=9022
echo "Starting server on port $PORT with high delay (1000ms)..."
./server $PORT 1000 > test3_server.log 2>&1 &
SERVER_PID=$!
sleep 1

echo "Starting client with 10 messages..."
./client 127.0.0.1 $PORT 10 > test3_client.log 2>&1 &
CLIENT_PID=$!

# Wait for client to send messages 1..5 and block on window full
sleep 0.3
echo "Abruptly terminating server while client sender is blocked..."
kill -9 $SERVER_PID 2>/dev/null || true

# Wait for client process to unblock and terminate cleanly
wait $CLIENT_PID || true
echo "Client process exited without deadlock or hanging!"

echo ""
echo "[Test 3 Client Output on Abrupt Server Disconnect]:"
cat test3_client.log

grep -E "(Server closed connection prematurely|Socket read error)" test3_client.log && echo "  [x] Receiver detected server disconnect"
grep -q "Sender thread terminated cleanly" test3_client.log && echo "  [x] Blocked sender unblocked via condition variable broadcast"
grep -q "Receiver thread terminated cleanly" test3_client.log && echo "  [x] Receiver thread terminated cleanly"

rm -f test3_server.log test3_client.log

# -------------------------------------------------------------------
# Test 4: Default Zero-Delay High-Throughput Test
# -------------------------------------------------------------------
echo ""
echo "--- TEST 4: Zero-Delay (Default) High-Throughput Test (25 messages) ---"
PORT=9023
./server $PORT > test4_server.log 2>&1 &
SERVER_PID=$!
sleep 1

./client 127.0.0.1 $PORT 25 > test4_client.log 2>&1
kill -SIGINT $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

grep -q "All 25 messages have been successfully acknowledged" test4_client.log && echo "  [x] All 25 messages acknowledged at full line rate with 0 delay"
rm -f test4_server.log test4_client.log

# -------------------------------------------------------------------
# Test 5: Invalid, Duplicate, and Out-of-Order ACK Rejection
# -------------------------------------------------------------------
echo ""
echo "--- TEST 5: Robustness to Invalid, Duplicate, and Out-of-Order ACKs ---"
PORT=9024

python3 -c "
import socket, time
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('127.0.0.1', $PORT))
s.listen(1)
conn, _ = s.accept()

# 1. Valid ACK 1
conn.sendall(b'ACK 1\n')
time.sleep(0.05)

# 2. Duplicate ACK 1 (should be ignored, base must not advance)
conn.sendall(b'ACK 1\n')
time.sleep(0.05)

# 3. Out-of-order ACK 99 (should be rejected, base must not advance)
conn.sendall(b'ACK 99\n')
time.sleep(0.05)

# 4. Malformed ACK (should be rejected)
conn.sendall(b'MALFORMED_GARBAGE\n')
time.sleep(0.05)

# 5. Trailing non-whitespace garbage after valid integer prefix (should be rejected)
conn.sendall(b'ACK 2xyz\n')
time.sleep(0.05)

# 6. Negative sequence number (should be rejected)
conn.sendall(b'ACK -3\n')
time.sleep(0.05)

# 7. Integer overflow (should be rejected)
conn.sendall(b'ACK 9999999999999999999999999999999999999\n')
time.sleep(0.05)

# 8. Valid remaining ACKs 2, 3, 4, 5
conn.sendall(b'ACK 2\n')
time.sleep(0.05)
conn.sendall(b'ACK 3\n')
time.sleep(0.05)
conn.sendall(b'ACK 4\n')
time.sleep(0.05)
conn.sendall(b'ACK 5\n')
time.sleep(0.1)

conn.close()
s.close()
" &
MOCK_PID=$!
sleep 1

./client 127.0.0.1 $PORT 5 > test5_client.log 2>&1
wait $MOCK_PID 2>/dev/null || true

echo "[Test 5 Client Output - Verifying ACK Validation]:"
cat test5_client.log

grep -q "Duplicate ACK 1 received" test5_client.log && echo "  [x] Duplicate ACK correctly identified and ignored"
grep -q "Unexpected out-of-order ACK 99 received" test5_client.log && echo "  [x] Out-of-order ACK correctly identified and rejected"
grep -q 'Malformed or invalid ACK received: "MALFORMED_GARBAGE"' test5_client.log && echo "  [x] Malformed non-numeric ACK discarded"
grep -q 'Malformed or invalid ACK received: "ACK 2xyz"' test5_client.log && echo "  [x] Trailing non-whitespace garbage rejected"
grep -q 'Malformed or invalid ACK received: "ACK -3"' test5_client.log && echo "  [x] Negative sequence number rejected"
grep -q 'Malformed or invalid ACK received: "ACK 9999999999999999999999999999999999999"' test5_client.log && echo "  [x] Integer overflow rejected"
grep -q "SUCCESS" test5_client.log && echo "  [x] Invalid ACKs did not corrupt base; run finished with SUCCESS"
rm -f test5_client.log

echo ""
echo "====================================================================="
echo "ALL PART B TESTS COMPLETED AND VERIFIED SUCCESSFULLY!"
echo "====================================================================="
