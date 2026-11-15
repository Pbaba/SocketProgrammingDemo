# Computer Networks Lab Assignment — C Socket Programming

**Course:** Computer Networks (CSD 304)

This project contains two programs written in C that demonstrate how computers communicate using TCP sockets.

- **Part A:** A client sends messages to a server and receives acknowledgements. The server can handle multiple clients at the same time.
- **Part B:** A client uses a sliding window to send several messages before having to wait for acknowledgements.

Both parts use TCP sockets and POSIX threads.

---

## Project Structure

```text
CN_Lab_Assignment/
├── Part_A/
│   ├── client.c
│   ├── server.c
│   ├── Makefile
│   ├── test_part_a.sh
│   └── test_part_a_advanced.sh
│
├── Part_B/
│   ├── client.c
│   ├── server.c
│   ├── Makefile
│   └── test_part_b.sh
│
└── README.md
```

### What are these files?

- `client.c`: Contains the code that sends messages to the server.
- `server.c`: Contains the code that receives messages and sends replies.
- `Makefile`: Contains the commands used to compile the programs.
- `test_*.sh`: Scripts that run tests automatically to check whether the programs work correctly.
- `README.md`: Explains the project and how to run it.

---

## Requirements

The project was developed and tested using:

- **Operating System:** Ubuntu 24.04 through WSL2 on Windows 11.
- **Compiler:** GCC 15.2.0.
- **Programming Language:** C (C11).
- **Networking:** TCP sockets.
- **Multithreading:** POSIX Threads (`pthread`).

No additional networking libraries are required.

---

# PART A — Client and Concurrent Server

## 1. What does it do?

Part A demonstrates basic communication between a client and a server using TCP sockets.

The client sends a message containing `hello` and waits for the server to reply with `ACK`.

The server receives the message, prints it on the terminal and sends an ACK back to the client.

This exchange can be repeated multiple times.

The server can also handle multiple clients at the same time instead of making each client wait for the previous one to finish.

## 2. How does it work?

The communication follows these steps:

1. The server starts and waits for clients to connect.
2. A client connects to the server.
3. The client sends `hello`.
4. The server receives and prints the message.
5. The server sends `ACK` back to the client.
6. The client receives the ACK and can send another message.

### How does the server handle multiple clients?

Whenever a new client connects, the server creates a separate POSIX thread using `pthread`.

Think of each thread as a separate worker. Each worker handles one client, allowing several clients to communicate with the server at the same time.

For example, if three clients connect, the server can create three threads to handle them independently.

## 3. Handling TCP messages

TCP sends data as a continuous stream of bytes. It does not automatically separate the data into individual messages.

To make sure the program knows where each message ends, this implementation uses newline characters (`\n`).

For example:

```text
hello\n
```

The server reads the incoming data until it finds the newline character. It then treats everything before it as one complete message.

If a client disconnects before sending the newline, the server recognises that the message is incomplete and does not send an ACK for it.

## 4. Handling server shutdown

When the server receives `Ctrl+C`, it begins shutting down.

It stops accepting new clients, closes the active client connections and waits for the worker threads to finish.

This prevents threads from being left running after the server has stopped.

The implementation uses a technique called the **self-pipe trick** to notify the main server thread that a shutdown signal has been received.

## 5. Compilation

Open a terminal and navigate to the Part A folder:

```bash
cd Part_A
make clean && make
```

The `make` command reads the Makefile and compiles the client and server programs.

`make clean` removes the previously compiled programs so that they can be built again from scratch.

## 6. Running the programs

**Start the server:**

```bash
./server [port]
```

- Default port: `8080`.
- You can provide a different port if required.

**Start the client in another terminal:**

```bash
./client [server_ip] [port] [message_count]
```

Default values:

- Server IP: `127.0.0.1`
- Port: `8080`
- Message count: `0` (continues sending messages until stopped with `Ctrl+C`).

For example:

```bash
./client 127.0.0.1 8080 5
```

This connects to the server running on the same machine and sends five messages.

## 7. Testing

Run the advanced test script:

```bash
chmod +x test_part_a_advanced.sh
./test_part_a_advanced.sh
```

The tests check whether:

- A client can successfully send a message and receive an ACK.
- Multiple clients can communicate with the server at the same time.
- Incomplete messages are detected.
- One client disconnecting does not affect other clients.
- The server shuts down properly when `Ctrl+C` is pressed.

---

# PART B — Sliding Window Protocol

## 1. What does it do?

Part B builds on Part A by introducing a **sliding window protocol**.

Instead of sending one message and waiting for its ACK before sending the next one, the client can send up to five messages without waiting.

The client then waits for acknowledgements. Every time it receives a valid ACK for the oldest unacknowledged message, the window moves forward by one position.

The window size is fixed at **5 messages**.

## 2. How does it work?

The client and server exchange messages using sequence numbers.

For example:

```text
HELLO 1
HELLO 2
HELLO 3
HELLO 4
HELLO 5
```

The server replies with acknowledgements containing the corresponding sequence numbers:

```text
ACK 1
ACK 2
ACK 3
ACK 4
ACK 5
```

The sequence number helps the client identify which message the server has acknowledged.

### Understanding the sliding window

Imagine that the client has five slots available.

Initially, it can send messages 1 to 5. Once all five slots are occupied, it cannot send another message until it receives an ACK.

When `ACK 1` arrives, the first slot becomes available. The window moves forward, allowing the client to send message 6.

When `ACK 2` arrives, another slot becomes available, allowing message 7 to be sent.

This continues until all messages have been sent and acknowledged.

## 3. Sender and Receiver Threads

The Part B client uses two separate POSIX threads.

**Sender Thread**

- Responsible for sending messages to the server.
- Checks whether there is space in the sliding window.
- Sends messages while fewer than five messages are waiting for ACKs.
- Waits if the window is full.

**Receiver Thread**

- Continuously listens for ACKs from the server.
- Checks whether each ACK is for the oldest message waiting to be acknowledged.
- Moves the window forward when a valid ACK arrives.
- Wakes up the sender thread when space becomes available.

The receiver thread starts before the sender thread so that it is ready to receive ACKs as soon as messages are sent.

## 4. Synchronisation Between Threads

Since the sender and receiver threads both access information about the sliding window, they need to coordinate their work.

The implementation uses:

- **Mutex:** Makes sure that only one thread accesses or changes shared information at a time.
- **Condition Variable:** Allows the sender thread to wait when the window is full and wake up when an ACK creates space.

For example, if five messages have been sent and none have been acknowledged, the sender waits on a condition variable.

When the receiver gets a valid ACK, it updates the window and signals the sender to continue.

If the server disconnects or an error occurs, the receiver informs the sender and wakes it up. This prevents the sender from waiting forever.

## 5. ACK Validation

The client does not accept every ACK it receives.

It checks whether the ACK matches the oldest message that has not yet been acknowledged.

For example, if the client is waiting for `ACK 2` but receives `ACK 1` again, it ignores the duplicate ACK.

It also rejects:

- ACKs that arrive out of order.
- ACKs with invalid sequence numbers.
- Malformed ACK messages.

This prevents incorrect acknowledgements from moving the window forward.

## 6. Compilation

Navigate to the Part B folder:

```bash
cd Part_B
make clean && make
```

This compiles the client and server using the commands in the Part B Makefile.

## 7. Running the programs

**Start the server:**

```bash
./server [port] [delay_ms]
```

Default values:

- Port: `8080`
- Artificial delay: `0` milliseconds.

You can add a delay of 20–40 milliseconds to make it easier to observe how the window moves as ACKs arrive.

**Start the client in another terminal:**

```bash
./client [server_ip] [port] [total_messages]
```

Default values:

- Server IP: `127.0.0.1`
- Port: `8080`
- Total messages: `15`

For example:

```bash
./client 127.0.0.1 8080 15
```

This sends 15 messages using a sliding window of five.

## 8. Testing

Run the Part B test script:

```bash
chmod +x test_part_b.sh
./test_part_b.sh
```

The tests check whether:

- The client initially sends five messages.
- The sender waits when the window is full.
- Receiving an ACK allows the next message to be sent.
- Duplicate, out-of-order and malformed ACKs are rejected.
- All messages are eventually acknowledged.
- Multiple clients can use the server at the same time.
- The client handles server disconnection without getting stuck.

---

# Testing Summary

The following tests were performed according to the test scripts.

## Part A

- [x] Client successfully sends `hello` and receives `ACK`.
- [x] Multiple clients communicate with the server simultaneously.
- [x] Incomplete messages are detected and discarded.
- [x] Disconnecting one client does not interrupt other clients.
- [x] Server shuts down cleanly when `Ctrl+C` is pressed.

## Part B

- [x] Client sends five messages before waiting for ACKs.
- [x] Sender waits when all five window positions are occupied.
- [x] Each valid ACK moves the window forward by one position.
- [x] Duplicate, out-of-order and malformed ACKs are rejected.
- [x] All 15 messages are successfully sent and acknowledged.
- [x] Multiple clients can use the server simultaneously.
- [x] Server disconnection wakes up the waiting sender thread.

---

## Conclusion

This assignment demonstrates two important concepts in computer networks.

**Part A** shows how TCP sockets allow a client and server to communicate, and how multithreading allows a server to handle multiple clients simultaneously.

**Part B** introduces the sliding window protocol, showing how multiple messages can be sent before waiting for acknowledgements. It also demonstrates how mutexes and condition variables help coordinate multiple threads working on the same data.

Together, the two parts provide practical experience with TCP communication, sockets, multithreading and synchronisation in C.
