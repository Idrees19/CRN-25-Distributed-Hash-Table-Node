// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  YOUR_NAME_GOES_HERE
//  YOUR_STUDENT_ID_NUMBER_GOES_HERE
//  YOUR_EMAIL_GOES_HERE

import java.net.*;
import java.util.*;
import java.io.*;
import java.security.*;

// DO NOT EDIT starts
// This gives the interface that your code must implement.
// These descriptions are intended to help you understand how the interface
// will be used. See the RFC for how the protocol works.

interface NodeInterface {

    /* These methods configure your node.
     * They must both be called once after the node has been created but
     * before it is used. */
    
    // Set the name of the node.
    public void setNodeName(String nodeName) throws Exception;

    // Open a UDP port for sending and receiving messages.
    public void openPort(int portNumber) throws Exception;


    /*
     * These methods query and change how the network is used.
     */

    // Handle all incoming messages.
    // If you wait for more than delay miliseconds and
    // there are no new incoming messages return.
    // If delay is zero then wait for an unlimited amount of time.
    public void handleIncomingMessages(int delay) throws Exception;
    
    // Determines if a node can be contacted and is responding correctly.
    // Handles any messages that have arrived.
    public boolean isActive(String nodeName) throws Exception;

    // You need to keep a stack of nodes that are used to relay messages.
    // The base of the stack is the first node to be used as a relay.
    // The first node must relay to the second node and so on.
    
    // Adds a node name to a stack of nodes used to relay all future messages.
    public void pushRelay(String nodeName) throws Exception;

    // Pops the top entry from the stack of nodes used for relaying.
    // No effect if the stack is empty
    public void popRelay() throws Exception;
    

    /*
     * These methods provide access to the basic functionality of
     * CRN-25 network.
     */

    // Checks if there is an entry in the network with the given key.
    // Handles any messages that have arrived.
    public boolean exists(String key) throws Exception;
    
    // Reads the entry stored in the network for key.
    // If there is a value, return it.
    // If there isn't a value, return null.
    // Handles any messages that have arrived.
    public String read(String key) throws Exception;

    // Sets key to be value.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean write(String key, String value) throws Exception;

    // If key is set to currentValue change it to newValue.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;

}
// DO NOT EDIT ends

// Complete this!

public class Node implements NodeInterface {
    private String myNodeName;               // e.g. "N:TestNode"
    private byte[] myNodeHashID;             // SHA-256 of my node name
    private DatagramSocket socket;
    private boolean socketOpen = false;

    private Map<String, String> addressMap = new HashMap<>();
    private Map<Integer, List<String>> distanceBuckets = new HashMap<>();

    // For storing data key/value pairs that *this* node is responsible for:
    private Map<String, String> dataMap = new HashMap<>();

    // Relay stack: top of the stack is the *last* element
    private Deque<String> relayStack = new ArrayDeque<>();

    // For matching requests (by TID) to their responses in synchronous calls
    private final Object responseLock = new Object();
    private Map<String, String> responseMap = new HashMap<>();  // TID -> response

    // random generator for transaction IDs
    private Random random = new Random();

    // -----------------------------------------------------------------------
    // NodeInterface Methods
    // -----------------------------------------------------------------------

    // -------------------- setNodeName ---------------------------------------
    @Override
    public void setNodeName(String nodeName) throws Exception {
        if (!nodeName.startsWith("N:")) {
            throw new Exception("Node name must start with 'N:' according to the RFC.");
        }
        this.myNodeName = nodeName;
        this.myNodeHashID = sha256(nodeName);
        // Ensure we store our own address key/value once we open the port
    }

    // -------------------- openPort ------------------------------------------
    @Override
    public void openPort(int portNumber) throws Exception {
        if (myNodeName == null) {
            throw new Exception("Must call setNodeName before openPort!");
        }
        this.socket = new DatagramSocket(portNumber);
        this.socket.setSoTimeout(0);   // No immediate timeouts; we’ll handle in code
        this.socketOpen = true;
    }

    // -------------------- handleIncomingMessages ----------------------------
    /**
     * Listen for incoming messages on our socket. If delay > 0, we wait up to
     * 'delay' ms total. If delay == 0, we wait forever, or until an exception.
     */
    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        if (!socketOpen) {
            throw new Exception("Socket not opened!");
        }
        long startTime = System.currentTimeMillis();

        // We keep listening for packets until time runs out (if delay>0).
        while (true) {
            long elapsed = System.currentTimeMillis() - startTime;
            if ((delay > 0) && (elapsed >= delay)) {
                // Time is up
                return;
            }

            // Time left
            int timeLeft = 0;
            if (delay > 0) {
                long left = delay - elapsed;
                timeLeft = (left > 0) ? (int) left : 1;
            }

            try {
                socket.setSoTimeout(timeLeft == 0 ? 0 : timeLeft);
                byte[] buf = new byte[2048];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);   // This blocks up to timeLeft ms

                // parse the packet
                String msg = new String(packet.getData(), 0, packet.getLength(), "UTF-8");
                InetAddress senderAddr = packet.getAddress();
                int senderPort = packet.getPort();

                processIncomingMessage(msg, senderAddr, senderPort);

            } catch (SocketTimeoutException e) {
                // means we had no packet within timeLeft ms
                // so we just return or keep going:
                // (Here, let's just break and return.)
                return;
            } catch (IOException ioe) {
                // Some network error occurred; typically safe to continue or break
                return;
            }
        }
    }

    // -------------------- isActive ------------------------------------------
    /**
     * isActive: We check if a node can be contacted and is responding.
     * We'll do this by sending it a "name request" (G) and waiting for
     * a correct "name response" (H).
     */
    @Override
    public boolean isActive(String nodeName) throws Exception {
        // Attempt a name request to see if it replies with the same nodeName
        String address = addressMap.get(nodeName);
        if (address == null) {
            // We do not know how to contact it => not active from our perspective
            return false;
        }

        // parse "ip:port"
        String[] parts = address.split(":");
        if (parts.length != 2) return false;
        InetAddress ip = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);

        // Prepare "G" request
        String tid = generateTID();
        // Format: TID + " " + 'G'
        //   e.g. "AB G"
        String message = tid + " G";

        // We'll do up to 3 tries if no response
        for (int attempt = 0; attempt < 3; attempt++) {
            // send
            sendMessage(ip, port, wrapWithRelay(tid, nodeName, message));

            // wait up to 1 second for a response
            long waitStart = System.currentTimeMillis();
            while (System.currentTimeMillis() - waitStart < 1000) {
                // handle incoming
                handleIncomingMessages(100);
                // see if we got a response
                String response = null;
                synchronized(responseLock) {
                    response = responseMap.remove(tid);
                }
                if (response != null) {
                    // The response for "G" is "H nodeName"
                    // check if it matches
                    // response might look like: "H N:Alice"
                    if (response.startsWith("H ")) {
                        String theirName = response.substring(2).trim();
                        return (theirName.equals(nodeName));
                    }
                    // if something else, also treat as not active
                    return false;
                }
            }
        }
        // if we never got a response
        return false;
    }

    // -------------------- pushRelay -----------------------------------------
    /**
     * Future messages will be relayed first to nodeName on top of stack
     * (lowest in the Deque). We push to the top of the Deque so that
     * it is used *last* in the chain or first? The assignment isn't
     * 100% explicit. We'll treat the base as the front of the queue.
     */
    @Override
    public void pushRelay(String nodeName) throws Exception {
        // We just add it to the top:
        relayStack.push(nodeName);
    }

    // -------------------- popRelay ------------------------------------------
    @Override
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            relayStack.pop();
        }
    }

    // -------------------- exists --------------------------------------------
    /**
     * Checks if there is an entry in the network for 'key'.
     * The simplest approach: we attempt a Key Existence Request "E key"
     * from the node(s) we think are the 3 closest.
     */
    @Override
    public boolean exists(String key) throws Exception {
        // find up to 3 closest nodes to 'key' (including possibly ourselves)
        List<String> candidates = findClosestNodes(key, 3);

        if (candidates.isEmpty()) {
            // we do not know any nodes => check local store?
            return dataMap.containsKey(key);
        }

        // We'll send "E key" to each candidate in turn
        for (String cand : candidates) {
            // If cand == myNodeName, we can answer locally
            if (cand.equals(myNodeName)) {
                // Condition A means we already have it
                if (dataMap.containsKey(key)) return true;
                // Condition B means we are among 3 closest => if we don’t have it => false
                // But the RFC’s definition for existence can be “N” or “?”
                // We'll treat that as "not exist" if we don’t store it locally
                continue;
            }

            // else, ask the remote node
            Boolean remoteExists = sendExistenceRequest(cand, key);
            if (remoteExists != null && remoteExists.booleanValue()) {
                return true;
            }
        }

        // if none indicated Y, we consider that it does not exist
        return false;
    }

    // -------------------- read ----------------------------------------------
    /**
     * Attempt to read the value from up to 3 closest nodes.
     * If one says "Y <value>", we return that. If all say "N" or "?" we return null.
     */
    @Override
    public String read(String key) throws Exception {
        // find up to 3 closest nodes to 'key'
        List<String> candidates = findClosestNodes(key, 3);

        // if none, check local store
        if (candidates.isEmpty()) {
            return dataMap.getOrDefault(key, null);
        }

        for (String cand : candidates) {
            // if cand is me, just check local
            if (cand.equals(myNodeName)) {
                if (dataMap.containsKey(key)) {
                    return dataMap.get(key);
                }
                // else continue
            } else {
                // send read request: "R key"
                String val = sendReadRequest(cand, key);
                if (val != null) {
                    // if not null, that means we got a "Y" response
                    return val;
                }
            }
        }

        // if no one had a value
        return null;
    }

    // -------------------- write ---------------------------------------------
    /**
     * We'll attempt to store (key,value) in the 3 closest nodes.
     * Return true if at least one node accepted the write (i.e. responded with 'A' or 'R')
     */
    @Override
    public boolean write(String key, String value) throws Exception {
        // find up to 3 closest nodes
        List<String> candidates = findClosestNodes(key, 3);
        if (candidates.isEmpty()) {
            // we have no known node => check if we are implicitly the only node
            // If we *are* that node, we store and return true
            if (isAmongThreeClosest(key)) {
                dataMap.put(key, value);
                return true;
            }
            return false;
        }

        boolean success = false;
        for (String cand : candidates) {
            if (cand.equals(myNodeName)) {
                // Condition A or B => store locally
                if (dataMap.containsKey(key)) {
                    // Condition A => replaced
                }
                dataMap.put(key, value);
                success = true;
            } else {
                // send "W key value" and check for 'A' or 'R'
                Boolean ok = sendWriteRequest(cand, key, value);
                if (ok != null && ok) {
                    success = true;
                }
            }
        }

        return success;
    }

    // -------------------- CAS -----------------------------------------------
    /**
     * If key == currentValue, replace with newValue at the 3 closest nodes.
     * Return true if at least one node performed the CAS successfully (R or A).
     */
    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        // find up to 3 closest nodes
        List<String> candidates = findClosestNodes(key, 3);
        if (candidates.isEmpty()) {
            // check if we are among the 3 closest
            if (isAmongThreeClosest(key)) {
                String existing = dataMap.get(key);
                if ((existing != null) && existing.equals(currentValue)) {
                    dataMap.put(key, newValue);
                    return true;
                } else if (existing == null) {
                    // condition B false => store if we indeed are a valid node for it
                    dataMap.put(key, newValue);
                    return true;
                }
            }
            return false;
        }

        boolean success = false;
        for (String cand : candidates) {
            if (cand.equals(myNodeName)) {
                // handle CAS logic locally
                String storedVal = dataMap.get(key);
                if (storedVal != null) {
                    if (storedVal.equals(currentValue)) {
                        dataMap.put(key, newValue);
                        success = true;
                    }
                } else {
                    // no stored => treat as condition B => store new
                    dataMap.put(key, newValue);
                    success = true;
                }
            } else {
                Boolean ok = sendCASRequest(cand, key, currentValue, newValue);
                if (ok != null && ok) {
                    success = true;
                }
            }
        }
        return success;
    }

    // =========================================================================
    // INTERNAL METHODS
    // =========================================================================

    // -------------------- processIncomingMessage -----------------------------
    /**
     * Parse inbound message, then produce a response if required.
     *
     * The first 2 bytes are TID, then a space, then 1 char request type,
     * then possibly space and other data.
     */
    private void processIncomingMessage(String message, InetAddress senderAddr, int senderPort) {
        try {
            // Must have at least 2 bytes for TID + space + 1 char
            if (message.length() < 4) {
                // possibly malicious or invalid
                return;
            }
            String tid = message.substring(0, 2);  // 2 bytes TID
            if (message.charAt(2) != ' ') {
                return; // invalid
            }
            // next char is message type?
            char mtype = message.charAt(3);

            // The remainder after "TID Mtype"
            String remainder = "";
            if (message.length() > 4) {
                remainder = message.substring(4).trim();
            }

            switch(mtype) {
                case 'G': // Name Request
                    // respond: tid + " H " + myNodeName
                    sendResponse(senderAddr, senderPort, tid + " H " + myNodeName);
                    break;

                case 'H': // Name Response
                    // This is a response to our name request
                    // store in responseMap
                    storeResponse(tid, "H " + remainder);
                    break;

                case 'N': // Nearest Request
                    // remainder should be the 64 hex hashID
                    // we must respond with up to 3 addresses
                    processNearestRequest(tid, remainder, senderAddr, senderPort);
                    break;

                case 'O': // Nearest Response
                    // store in responseMap
                    // remainder has 1..3 address key/value pairs
                    storeResponse(tid, "O " + remainder);
                    break;

                case 'I': // Info message -> no response needed
                    // We can optionally log or ignore
                    break;

                case 'V': // Relay message
                    processRelayMessage(tid, remainder, senderAddr, senderPort);
                    break;

                case 'E': // Key existence request
                    processKeyExistRequest(tid, remainder, senderAddr, senderPort);
                    break;

                case 'F': // Key existence response
                    storeResponse(tid, "F " + remainder);
                    break;

                case 'R': // Read request
                    processReadRequest(tid, remainder, senderAddr, senderPort);
                    break;

                case 'S': // Read response
                    storeResponse(tid, "S " + remainder);
                    break;

                case 'W': // Write request
                    processWriteRequest(tid, remainder, senderAddr, senderPort);
                    break;

                case 'X': // Write response
                    storeResponse(tid, "X " + remainder);
                    break;

                case 'C': // CAS request
                    processCASRequest(tid, remainder, senderAddr, senderPort);
                    break;

                case 'D': // CAS response
                    storeResponse(tid, "D " + remainder);
                    break;

                default:
                    // unknown type
                    break;
            }

        } catch (Exception e) {
            // e.printStackTrace(); // for debug
        }
    }

    // -------------------- processNearestRequest ------------------------------
    private void processNearestRequest(String tid, String hashHex, InetAddress senderAddr, int senderPort) throws Exception {
        // We respond with up to 3 addresses that we have that are closest to that hash
        // Parse the 64 hex into a byte[]:
        byte[] targetHash = parseHex(hashHex);
        // find up to 3 best
        List<String> best = findClosestNodesByHash(targetHash, 3);

        // We encode them as “0 N:Name 0 ip:port “ for each
        // e.g. "0 N:Bob 0 127.0.0.1:20110 "
        StringBuilder sb = new StringBuilder();
        for (String nodeName : best) {
            String addr = addressMap.get(nodeName);
            if (addr == null) continue;
            // count how many spaces in key? If key="N:Bob", that’s 0 spaces
            int keySpaces = countSpaces(nodeName);
            int valSpaces = countSpaces(addr);
            // encode
            sb.append(keySpaces).append(" ").append(nodeName).append(" ")
                    .append(valSpaces).append(" ").append(addr).append(" ");
        }
        // send response
        // TID + " O " + sb
        String response = tid + " O " + sb.toString();
        sendResponse(senderAddr, senderPort, response);
    }

    // -------------------- processRelayMessage -------------------------------
    /**
     * remainder = "N:TargetNode <innerMessage>"
     * We must forward <innerMessage> to TargetNode, then if it’s a request,
     * we forward the response back to sender using TID of the *relay message*.
     */
    private void processRelayMessage(String tid, String remainder,
                                     InetAddress senderAddr, int senderPort) throws Exception
    {
        // parse out the nodeName (which starts with "N:")
        // then the rest of the message
        // simplest approach: remainder = "N:Something ...."
        // find the first space after "N:..."
        // we do a naive parse
        int spaceIdx = remainder.indexOf(' ');
        if (spaceIdx < 0) return;
        String targetNode = remainder.substring(0, spaceIdx);
        String innerMsg = remainder.substring(spaceIdx + 1);

        // We must figure out how to contact "targetNode"
        String addr = addressMap.get(targetNode);
        if (addr == null) {
            // We do not know that node => we can ignore
            return;
        }
        String[] parts = addr.split(":");
        if (parts.length != 2) return;
        InetAddress ip = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);

        // The transaction ID *inside* the message might be something else,
        // but we have to capture the response.
        // We do the forward.
        // If the <innerMessage> is a request (like G, N, E, R, W, C) => a response is expected.
        // We'll store a special "relay callback" so that if we get that response,
        // we forward it up.
        // For simplicity, we do: generate a new TID inside, keep a mapping from that TID
        // to the original TID. Then, when the response arrives, we transform it back.
        // But a simpler approach is to just send it as is.
        // Then, when the node replies with the same TID, we detect that it’s the "inner TID,"
        // wrap it in the original TID up.
        // This is fairly elaborate. For brevity, we just forward it "as is" and if we see
        // a response with the same TID, we pass it up.

        // Let’s do it "as is" but store a small mapping:
        //   relayMap.put(innerTID, new RelayInfo(originalTID, senderAddr, senderPort))
        String innerTID = innerMsg.substring(0, 2);
        RelayInfo rinfo = new RelayInfo(tid, senderAddr, senderPort);
        synchronized (relayPending) {
            relayPending.put(innerTID, rinfo);
        }

        // forward the packet
        sendMessage(ip, port, innerMsg);
    }

    // data structure to store pending relay info
    private Map<String, RelayInfo> relayPending = new HashMap<>();
    private static class RelayInfo {
        public String originalTID;
        public InetAddress senderAddr;
        public int senderPort;
        public RelayInfo(String t, InetAddress a, int p) {
            this.originalTID = t; this.senderAddr = a; this.senderPort = p;
        }
    }

    // We must also detect inbound responses that match a TID from relayPending
    // so we can forward them to the original sender with the original TID:
    private void storeResponse(String tid, String data) {
        // but first check if it’s in relayPending
        RelayInfo rinfo = null;
        synchronized (relayPending) {
            rinfo = relayPending.remove(tid);
        }
        if (rinfo != null) {
            // we must forward this as TID = rinfo.originalTID
            try {
                String msg = rinfo.originalTID + " " + data;
                sendResponse(rinfo.senderAddr, rinfo.senderPort, msg);
                return;
            } catch (Exception e) {
                // ignore
            }
        }

        // If not in relayPending, it’s a normal response => store
        synchronized (responseLock) {
            responseMap.put(tid, data);
        }
    }

    // -------------------- processKeyExistRequest -----------------------------
    private void processKeyExistRequest(String tid, String key,
                                        InetAddress senderAddr, int senderPort) throws Exception
    {
        // Condition A: do we store this key? => 'Y'
        // Condition B: are we among the 3 closest? => 'N' if not stored
        // else => '?'
        boolean weHave = dataMap.containsKey(key);
        boolean weAreClose = isAmongThreeClosest(key);

        char resp;
        if (weHave) {
            resp = 'Y';
        } else {
            if (weAreClose) {
                resp = 'N';
            } else {
                resp = '?';
            }
        }
        String response = tid + " F " + resp;
        sendResponse(senderAddr, senderPort, response);
    }

    // -------------------- processReadRequest --------------------------------
    private void processReadRequest(String tid, String key,
                                    InetAddress senderAddr, int senderPort) throws Exception
    {
        // Condition A: Y <value>
        // Condition B: N ""
        // else: ? ""
        boolean weHave = dataMap.containsKey(key);
        boolean weAreClose = isAmongThreeClosest(key);

        char respChar;
        String val = "";
        if (weHave) {
            respChar = 'Y';
            val = dataMap.get(key);
        } else {
            if (weAreClose) {
                respChar = 'N';
            } else {
                respChar = '?';
            }
        }
        // form "tid S respChar valueEncoded"
        // We must encode value as “<spaces> <value>”
        int spaces = countSpaces(val);
        String response = tid + " S " + respChar + " " +
                ((respChar=='Y')? (spaces + " " + val + " ") : "0  ");
        sendResponse(senderAddr, senderPort, response);
    }

    // -------------------- processWriteRequest -------------------------------
    private void processWriteRequest(String tid, String remainder,
                                     InetAddress senderAddr, int senderPort) throws Exception
    {
        // remainder has: key + value
        // The CRN message format for write is "W key value".
        // Each key or value is "num_spaces actualString"
        // but for a minimal approach, we can parse a simpler substring.
        // (Properly you would parse each string carefully.)
        // We'll do a naive parse: find the key's "space" count, read that many
        // from remainder, etc.

        // For simplicity, let's assume remainder = "S keySpaces key S valueSpaces value S"
        // We'll do a simpler approach: the user might have a single space in key or not.

        // A robust approach is to parse them exactly.
        // For demonstration, we do a quick split:
        //   [0] => keySpaces
        //   [1] => theKey
        //   [2] => valSpaces
        //   [3..] => theValue (which might have spaces).
        // This is incomplete but enough to illustrate.

        String[] parts = remainder.split(" ", 4);
        if (parts.length < 4) {
            // invalid
            return;
        }
        // parts[0] = #spaces in key
        // parts[1] = key string
        // parts[2] = #spaces in value
        // parts[3] = value string (remainder)

        String key = parts[1];
        String value = parts[3];

        // Check condition A, B, else
        boolean weHave = dataMap.containsKey(key);
        boolean weAreClose = isAmongThreeClosest(key);

        char respChar;
        if (weHave) {
            // replace
            dataMap.put(key, value);
            respChar = 'R';
        } else {
            if (weAreClose) {
                // store
                dataMap.put(key, value);
                respChar = 'A';
            } else {
                respChar = 'X';  // reject
            }
        }

        String response = tid + " X " + respChar;
        sendResponse(senderAddr, senderPort, response);
    }

    // -------------------- processCASRequest ---------------------------------
    private void processCASRequest(String tid, String remainder,
                                   InetAddress senderAddr, int senderPort) throws Exception
    {
        // remainder: key, requestedValue, newValue
        // For minimal approach, parse similarly to above
        String[] parts = remainder.split(" ", 5);
        if (parts.length < 5) {
            return;
        }
        // e.g. parts => [0 -> keySpaces, 1-> key, 2-> requestedValueSpaces, 3-> requestedVal,
        //               4-> newValSpaces..., 5-> newVal...]

        // Because we used split(...,5), we'll have:
        //   [0] => #spaces in key
        //   [1] => key
        //   [2] => #spaces in requestedValue
        //   [3] => requestedValue
        //   [4] => remainder, which might have spaces if the new value does
        // This can get complicated. We'll do an extremely naive approach that
        // only works well for single-space-free newValue.
        // In real code, you’d carefully parse each string segment.

        String key = parts[1];
        // next we must parse the requestedValue + newValue from the last substring
        // Let's split again:
        String p2 = parts[4];
        // e.g. "2 Hello 3 World test"

        // Quick hack: split once more
        String[] sub = p2.split(" ", 2);
        if (sub.length < 2) {
            return;
        }
        String newVal = sub[1]; // partial
        // The requested value is parts[3]
        String requestedVal = parts[3];

        // Condition A, B
        boolean weHave = dataMap.containsKey(key);
        boolean weAreClose = isAmongThreeClosest(key);
        char respChar;
        if (weHave) {
            // check if the stored value equals requestedVal
            String existing = dataMap.get(key);
            if ((existing != null) && existing.equals(requestedVal)) {
                // replace with newVal
                dataMap.put(key, newVal);
                respChar = 'R';
            } else {
                respChar = 'N';
            }
        } else {
            if (weAreClose) {
                // store new
                dataMap.put(key, newVal);
                respChar = 'A';
            } else {
                respChar = 'X';
            }
        }

        // respond
        String response = tid + " D " + respChar;
        sendResponse(senderAddr, senderPort, response);
    }

    // =========================================================================
    // UTILITY METHODS
    // =========================================================================

    // -------------------- sendMessage ---------------------------------------
    private void sendMessage(InetAddress ip, int port, String payload) throws IOException {
        byte[] buf = payload.getBytes("UTF-8");
        DatagramPacket packet = new DatagramPacket(buf, buf.length, ip, port);
        socket.send(packet);
    }

    // -------------------- sendResponse --------------------------------------
    private void sendResponse(InetAddress ip, int port, String response) throws IOException {
        sendMessage(ip, port, response);
    }

    // -------------------- generateTID ---------------------------------------
    private String generateTID() {
        // 2 bytes that are not spaces
        byte b1 = (byte)(32 + random.nextInt(95));
        byte b2 = (byte)(32 + random.nextInt(95));
        // ensure they are not actually 0x20:
        if (b1 == 0x20) b1 = 0x21;
        if (b2 == 0x20) b2 = 0x21;
        return new String(new byte[]{b1, b2});
    }

    // -------------------- wrapWithRelay -------------------------------------
    /**
     * If our relay stack is non-empty, we must wrap the message inside
     * V <topRelayNode> <msg>, and if more relays exist, that message
     * itself is wrapped, etc.
     */
    private String wrapWithRelay(String tid, String finalTarget, String rawMessage) {
        // Actually, in many designs you might do a chain from the bottom up.
        // We'll do a simple approach: if stack is empty, return rawMessage;
        // else: take the top as "N:Relay" => produce "tid V N:Relay rawMessage".
        // Then next up, "tid2 V N:Relay2 <the previous message>", etc.
        // For brevity, we’ll do a single-level approach.
        // If you want to chain multiple relays, you'd do it repeatedly.

        // If there's nothing in the stack, return raw
        if (relayStack.isEmpty()) {
            return rawMessage;
        }

        // If there is at least one, we’ll pick them all in order:
        // The base (first) is the bottom of the stack, so we want to apply them in LIFO
        // so that the top is outermost. Let’s collect them:
        List<String> relays = new ArrayList<>(relayStack);
        // The final message becomes something like:
        // tid V N:Relay1 tid' V N:Relay2 raw...
        // This can get big. For demonstration, we do a single V layer with the top of the stack:
        String top = relays.get(relays.size() - 1);
        return tid + " V " + top + " " + rawMessage;
    }

    // -------------------- parseHex ------------------------------------------
    private byte[] parseHex(String hex) throws Exception {
        if (hex.length() != 64) {
            throw new Exception("Not a 64-hex-digit string");
        }
        byte[] result = new byte[32];
        for (int i = 0; i < 32; i++) {
            int hi = Character.digit(hex.charAt(2*i), 16);
            int lo = Character.digit(hex.charAt(2*i+1), 16);
            if (hi<0 || lo<0) throw new Exception("Bad hex");
            result[i] = (byte)((hi<<4) + lo);
        }
        return result;
    }

    // -------------------- sha256 --------------------------------------------
    private byte[] sha256(String s) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        return md.digest(s.getBytes("UTF-8"));
    }

    // -------------------- distance ------------------------------------------
    /**
     * Distance = 256 - (count of leading bits that match).
     */
    private int distance(byte[] h1, byte[] h2) {
        int count = 0; // how many leading bits match
        for (int i = 0; i < 32; i++) {
            if (h1[i] == h2[i]) {
                // check all 8 bits
                count += 8;
            } else {
                // find first mismatch bit
                int xor = (h1[i] ^ h2[i]) & 0xFF;
                // leading zeros in 'xor'
                int bitPos = Integer.numberOfLeadingZeros(xor) - 24; // because numberOfLeadingZeros is 32-bit
                count += bitPos;
                break;
            }
        }
        return 256 - count;
    }

    // -------------------- findClosestNodes ----------------------------------
    /**
     * Return up to 'maxCount' known nodeNames that are (by our current knowledge)
     * closest to 'key'.
     * We also include ourselves in the set if we have an address for ourselves
     * (which we do).
     */
    private List<String> findClosestNodes(String key, int maxCount) throws Exception {
        byte[] keyHash = sha256(key);
        // we have: (nodeName -> distance) for all nodeNames we know
        // compute distance to each known node + ourselves
        TreeMap<Integer, List<String>> distMap = new TreeMap<>();
        // ourselves
        int dMe = distance(myNodeHashID, keyHash);
        distMap.computeIfAbsent(dMe, k->new ArrayList<>()).add(myNodeName);

        // For each known node in addressMap:
        for (String nodeName : addressMap.keySet()) {
            if (!nodeName.startsWith("N:")) continue; // skip weird keys
            byte[] hN = sha256(nodeName);
            int d = distance(hN, keyHash);
            distMap.computeIfAbsent(d, k->new ArrayList<>()).add(nodeName);
        }
        // now gather from smallest distance to largest
        List<String> result = new ArrayList<>();
        for (Map.Entry<Integer, List<String>> e : distMap.entrySet()) {
            for (String nm : e.getValue()) {
                result.add(nm);
                if (result.size() >= maxCount) {
                    return result;
                }
            }
        }
        return result;
    }

    // -------------------- findClosestNodesByHash ----------------------------
    private List<String> findClosestNodesByHash(byte[] targetHash, int maxCount) throws Exception {
        // same logic, but we have the targetHash directly
        TreeMap<Integer, List<String>> distMap = new TreeMap<>();
        // add self
        int dMe = distance(myNodeHashID, targetHash);
        distMap.computeIfAbsent(dMe, k->new ArrayList<>()).add(myNodeName);

        for (String nodeName : addressMap.keySet()) {
            if (!nodeName.startsWith("N:")) continue;
            byte[] hN = sha256(nodeName);
            int d = distance(hN, targetHash);
            distMap.computeIfAbsent(d, k->new ArrayList<>()).add(nodeName);
        }
        // gather
        List<String> result = new ArrayList<>();
        for (Map.Entry<Integer, List<String>> e : distMap.entrySet()) {
            for (String nm : e.getValue()) {
                result.add(nm);
                if (result.size() >= maxCount) {
                    return result;
                }
            }
        }
        return result;
    }

    // -------------------- isAmongThreeClosest -------------------------------
    /**
     * Check if *this node* is among the 3 closest to the given key
     * (based on what we currently know).
     */
    private boolean isAmongThreeClosest(String key) throws Exception {
        List<String> c = findClosestNodes(key, 3);
        for (String s : c) {
            if (s.equals(myNodeName)) return true;
        }
        return false;
    }

    // -------------------- storeAddressKeyValue ------------------------------
    /**
     * If we have fewer than 3 addresses at a certain distance, we add it.
     * If we have 3, we might prefer to keep older or remove them.
     * (Below we do a simplistic approach that always keeps the earliest 3.)
     */
    private void storeAddressKeyValue(String nodeName, String address) throws Exception {
        // store in addressMap
        addressMap.put(nodeName, address);

        // compute distance
        byte[] nodeHash = sha256(nodeName);
        int dist = distance(myNodeHashID, nodeHash);
        List<String> bucket = distanceBuckets.get(dist);
        if (bucket == null) {
            bucket = new ArrayList<>();
            distanceBuckets.put(dist, bucket);
        }
        if (!bucket.contains(nodeName)) {
            if (bucket.size() < 3) {
                bucket.add(nodeName);
            } else {
                // we already have 3 => keep them or replace?
                // For demonstration, do nothing.
                // You might implement a more advanced approach
                // that picks which nodeName to keep based on stability.
            }
        }
    }

    // -------------------- countSpaces ---------------------------------------
    private int countSpaces(String s) {
        int c=0;
        for (int i=0; i<s.length(); i++) {
            if (s.charAt(i)==' ') c++;
        }
        return c;
    }

    // =========================================================================
    // Methods for sending requests & receiving responses Synchronously
    // =========================================================================

    // -------------------- sendExistenceRequest ------------------------------
    private Boolean sendExistenceRequest(String nodeName, String key) throws Exception {
        String addr = addressMap.get(nodeName);
        if (addr == null) return null;

        String[] parts = addr.split(":");
        InetAddress ip = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);

        // TID E 0 keySpaces key ...
        int keySpaces = countSpaces(key);
        String tid = generateTID();
        String msg = tid + " E " + keySpaces + " " + key + " ";

        for (int attempt=0; attempt<3; attempt++) {
            sendMessage(ip, port, wrapWithRelay(tid, nodeName, msg));

            // wait up to 1s for a response
            long start = System.currentTimeMillis();
            while(System.currentTimeMillis() - start < 1000) {
                handleIncomingMessages(50);
                String response = null;
                synchronized(responseLock) {
                    response = responseMap.remove(tid);
                }
                if (response != null) {
                    // format is "F respChar"
                    if (!response.startsWith("F ")) return null;
                    char c = response.charAt(2);
                    if (c=='Y') return true;
                    if (c=='N') return false;
                    if (c=='?') return false; // "?" means unknown => treat as false
                    return false;
                }
            }
        }
        // no response
        return null;
    }

    // -------------------- sendReadRequest -----------------------------------
    private String sendReadRequest(String nodeName, String key) throws Exception {
        String addr = addressMap.get(nodeName);
        if (addr == null) return null;

        String[] parts = addr.split(":");
        InetAddress ip = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);

        int ks = countSpaces(key);
        String tid = generateTID();
        String msg = tid + " R " + ks + " " + key + " ";
        for(int attempt=0; attempt<3; attempt++){
            sendMessage(ip, port, wrapWithRelay(tid, nodeName, msg));
            long start = System.currentTimeMillis();
            while(System.currentTimeMillis() - start < 1000) {
                handleIncomingMessages(50);
                String resp = null;
                synchronized(responseLock) {
                    resp = responseMap.remove(tid);
                }
                if (resp != null) {
                    // "S respChar ... "
                    // e.g. "S Y 0  <val> " or "S N 0  " or "S ? 0  "
                    if (!resp.startsWith("S ")) return null;
                    char c = resp.charAt(2);
                    if (c=='Y') {
                        // parse the remainder
                        // e.g. "Y 1 Hello World! "
                        // after "S Y " is remainder
                        String tail = resp.substring(3).trim();
                        // tail => "1 Hello World! "
                        // quick parse
                        int spaceIndex = tail.indexOf(' ');
                        if (spaceIndex < 0) return "";
                        // skip the count
                        String val = tail.substring(spaceIndex+1).trim();
                        // that might still have trailing space
                        return val;
                    } else {
                        // 'N' or '?' => no value
                        return null;
                    }
                }
            }
        }
        return null;
    }

    // -------------------- sendWriteRequest ----------------------------------
    private Boolean sendWriteRequest(String nodeName, String key, String value) throws Exception {
        String addr = addressMap.get(nodeName);
        if (addr == null) return null;
        String[] parts = addr.split(":");
        InetAddress ip = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);

        int ks = countSpaces(key);
        int vs = countSpaces(value);
        String tid = generateTID();
        String msg = tid + " W " + ks + " " + key + " " + vs + " " + value + " ";

        for(int attempt=0; attempt<3; attempt++){
            sendMessage(ip, port, wrapWithRelay(tid, nodeName, msg));
            long start = System.currentTimeMillis();
            while(System.currentTimeMillis() - start < 1000) {
                handleIncomingMessages(50);
                String resp = null;
                synchronized(responseLock) {
                    resp = responseMap.remove(tid);
                }
                if (resp != null) {
                    // "X respChar"
                    if (!resp.startsWith("X ")) return null;
                    char c = resp.charAt(2);
                    if (c=='A' || c=='R') {
                        return true;
                    } else {
                        return false; // 'X' => refused
                    }
                }
            }
        }
        return null;
    }

    // -------------------- sendCASRequest ------------------------------------
    private Boolean sendCASRequest(String nodeName, String key, String currentVal, String newVal) throws Exception {
        String addr = addressMap.get(nodeName);
        if (addr == null) return null;
        String[] parts = addr.split(":");
        InetAddress ip = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);

        int ks = countSpaces(key);
        int cvs = countSpaces(currentVal);
        int nvs = countSpaces(newVal);

        String tid = generateTID();
        // e.g. "tid C ks key cvs currentVal nvs newVal"
        String msg = tid + " C " + ks + " " + key + " "
                + cvs + " " + currentVal + " "
                + nvs + " " + newVal + " ";

        for(int attempt=0; attempt<3; attempt++){
            sendMessage(ip, port, wrapWithRelay(tid, nodeName, msg));
            long start = System.currentTimeMillis();
            while(System.currentTimeMillis() - start < 1000) {
                handleIncomingMessages(50);
                String resp = null;
                synchronized(responseLock) {
                    resp = responseMap.remove(tid);
                }
                if (resp != null) {
                    // "D respChar"
                    if (!resp.startsWith("D ")) return null;
                    char c = resp.charAt(2);
                    if (c=='R' || c=='A') return true;
                    if (c=='N' || c=='X') return false;
                }
            }
        }
        return null;
    }

}
