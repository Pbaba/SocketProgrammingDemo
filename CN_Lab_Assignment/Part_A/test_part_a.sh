#!/usr/bin/env bash
set -e

PORT=9005
echo "=== Starting Part A Server on port $PORT ==="
./server $PORT > server.log 2>&1 &
SERVER_PID=$!
sleep 1

echo "=== Running Single-Client Test (3 messages) ==="
./client 127.0.0.1 $PORT 3

sleep 1

echo "=== Running Multi-Client Test (3 concurrent clients, 3 messages each) ==="
./client 127.0.0.1 $PORT 3 > client1.log 2>&1 &
PID1=$!
./client 127.0.0.1 $PORT 3 > client2.log 2>&1 &
PID2=$!
./client 127.0.0.1 $PORT 3 > client3.log 2>&1 &
PID3=$!

wait $PID1
wait $PID2
wait $PID3

echo "All 3 concurrent clients finished."
sleep 1

echo "=== Terminating Server ==="
kill -SIGINT $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo "=== Server Output Log ==="
cat server.log

echo "=== Client 1 Output Log ==="
cat client1.log

echo "=== Client 2 Output Log ==="
cat client2.log

echo "=== Client 3 Output Log ==="
cat client3.log

# Cleanup logs
rm -f server.log client1.log client2.log client3.log
echo "=== Part A Test Completed Successfully ==="

