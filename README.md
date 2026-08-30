# CRN-25 Distributed Hash Table Node
 
A Java implementation of a node in the CRN-25 peer-to-peer network protocol, built for the IN2011 Computer Networks coursework. Each node stores key/value pairs and cooperates with other nodes over UDP to route reads, writes, and compare-and-swap operations to whichever node(s) are "closest" to a given key, using SHA-256-based XOR distance (similar in spirit to Kademlia).
 
> This implements the `Node` side of the protocol against a fixed interface (`NodeInterface`) and message format defined by the coursework specification. `LocalTest.java` and `AzureLabTest.java` are the test harnesses provided with the assignment.
 
## Features
 
- **Full CRN-25 message handling** — recognises and responds to all message types: `W` (write), `R` (read), `E` (exists), `C` (compare-and-swap), `G` (get name), `N` (nearest), `V` (relay), and `I` (idle/ping).
- **Ownership logic (Condition A/B)** — determines whether a node already holds a key locally, or is among the 3 nodes closest to it, before deciding how to respond to read/write/exists/CAS requests.
- **Bounded address storage** — keeps at most 3 known node addresses per distance bucket.
- **Iterative closest-node lookup** — performs a breadth-first search across known peers to discover the actual nodes closest to a given key.
- **Automatic resending** — retries each outbound request up to 3 times if no response arrives within the timeout, per the protocol's resend requirement.
- **Relaying** — supports pushing/popping a stack of relay nodes so messages can be routed through intermediaries.
- **Local and remote reads/writes** — reads/writes data held locally, or forwards to the appropriate remote node(s) when it isn't the right owner.
## Project Structure
 
```
.
├── Node.java           # Core node implementation (NodeInterface)
├── HashID.java          # SHA-256 hash helper
├── LocalTest.java        # Spins up multiple nodes in-process and exercises read/write
└── AzureLabTest.java      # Single-node test harness for the Azure virtual lab
```
 
## Prerequisites
 
- Java 11+
- For `AzureLabTest`: access to the Azure virtual lab network (this test will **not** work from a personal machine, since it depends on reaching lab-hosted nodes)
## Build Instructions
 
1. Download or clone the repository, which should contain `Node.java`, `HashID.java`, `LocalTest.java`, and `AzureLabTest.java`.
2. Open a terminal in the project folder and compile everything:
```bash
   javac *.java
```
 
   This generates a `.class` file for each class.
 
## Running the Tests
 
### Local test (runs entirely on your own machine)
 
Spins up multiple in-process nodes, bootstraps them with each other's addresses, then writes and reads back several key/value pairs to confirm the network is functioning:
 
```bash
java LocalTest
```
 
Optionally specify the number of nodes (2–10):
 
```bash
java LocalTest 5
```
 
### Azure lab test (must be run on the virtual lab machines)
 
Starts a single node, waits to be contacted by others on the lab network, reads a known set of key/value pairs, writes a marker value, and then advertises itself to the network:
 
```bash
java AzureLabTest
```
 
Before running, set your own email address and the Azure lab machine's IP in `AzureLabTest.java` (search for the placeholder strings).
 
## Protocol Overview
 
Nodes communicate over UDP using short text-based messages, each starting with a 2-character transaction ID. Strings within a message are length-prefixed by the count of embedded spaces, so parsers can locate string boundaries reliably even when the string itself contains spaces.
 
Node/key "distance" is computed as `256 - (matching leading bits between two SHA-256 hashes)` — the closer two hashes are, the smaller the distance.
 
## Known Limitations / Notes
 
- **Message parsing assumes well-formed input.** Malformed messages generally result in silently dropped or `?`/`X` responses rather than detailed error reporting.
- **`HashID.java` is currently unused** — `Node.java` computes its own SHA-256 hash inline rather than calling `HashID.computeHashID`. Consider consolidating to one hashing implementation.
- **Fixed 3-node closeness threshold** is hardcoded in a few places (`Condition B`, address storage, nearest-node responses) rather than being a named constant.
- **No authentication/encryption** — as per the base protocol, messages are plain UDP text, so this is only suitable for a trusted lab network, not the open internet.
## Working Functionality Summary
 
- Message parsing and handling for all CRN message types (`W`, `R`, `E`, `C`, `G`, `N`, `V`, `I`)
- Condition A/B logic for read, write, exists, and CAS requests
- Enforced maximum of 3 stored addresses per distance
- Iterative nearest-node lookups via BFS
- Automatic resending of unacknowledged requests (up to 3 attempts)
- Local and remote read/write
- Atomic compare-and-swap
When run against `LocalTest` and `AzureLabTest`, the node successfully stores and retrieves data, handles address/key records, and passes the core functionality tests.
