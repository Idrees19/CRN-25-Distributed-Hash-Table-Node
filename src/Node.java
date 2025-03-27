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
    private String myNodeName;
    private byte[] myNodeHashID;
    private DatagramSocket socket;
    private boolean socketOpen = false;

    private Map<String, String> addressMap = new HashMap<>();
    private Map<Integer, List<String>> distanceBuckets = new HashMap<>();

    private Map<String, String> dataMap = new HashMap<>();

    private Deque<String> relayStack = new ArrayDeque<>();

    private final Object responseLock = new Object();
    private Map<String, String> responseMap = new HashMap<>();
    private Random random = new Random();

    @Override
    public void setNodeName(String nodeName) throws Exception {
        if (!nodeName.startsWith("N:")) {
            throw new Exception("Node name must start with 'N:' according to the RFC.");
        }
        this.myNodeName = nodeName;
        this.myNodeHashID = sha256(nodeName);
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        if (myNodeName == null) {
            throw new Exception("Must call setNodeName before openPort!");
        }
        this.socket = new DatagramSocket(portNumber);
        this.socket.setSoTimeout(0);
        this.socketOpen = true;
    }

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

        while (true) {
            long elapsed = System.currentTimeMillis() - startTime;
            if ((delay > 0) && (elapsed >= delay)) {
                return;
            }

            int timeLeft = 0;
            if (delay > 0) {
                long left = delay - elapsed;
                timeLeft = (left > 0) ? (int) left : 1;
            }

            try {
                socket.setSoTimeout(timeLeft == 0 ? 0 : timeLeft);
                byte[] buf = new byte[2048];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);

                String msg = new String(packet.getData(), 0, packet.getLength(), "UTF-8");
                InetAddress senderAddr = packet.getAddress();
                int senderPort = packet.getPort();

                processIncomingMessage(msg, senderAddr, senderPort);

            } catch (SocketTimeoutException e) {
                return;
            } catch (IOException ioe) {
                return;
            }
        }
    }

    /**
     * isActive: We check if a node can be contacted and is responding.
     * We'll do this by sending it a "name request" (G) and waiting for
     * a correct "name response" (H).
     */
    @Override
    public boolean isActive(String nodeName) throws Exception {
        String address = addressMap.get(nodeName);
        if (address == null) {
            return false;
        }

        String[] parts = address.split(":");
        if (parts.length != 2) return false;
        InetAddress ip = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);

        String tid = generateTID();
        String message = tid + " G";

        for (int attempt = 0; attempt < 3; attempt++) {
            sendMessage(ip, port, wrapWithRelay(tid, nodeName, message));

            long waitStart = System.currentTimeMillis();
            while (System.currentTimeMillis() - waitStart < 1000) {
                handleIncomingMessages(100);
                String response = null;
                synchronized(responseLock) {
                    response = responseMap.remove(tid);
                }
                if (response != null) {
                    if (response.startsWith("H ")) {
                        String theirName = response.substring(2).trim();
                        return (theirName.equals(nodeName));
                    }
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * Future messages will be relayed first to nodeName on top of stack
     * (lowest in the Deque). We push to the top of the Deque so that
     * it is used *last* in the chain or first? The assignment isn't
     * 100% explicit. We'll treat the base as the front of the queue.
     */
    @Override
    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
    }

    @Override
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            relayStack.pop();
        }
    }

    /**
     * Checks if there is an entry in the network for 'key'.
     * The simplest approach: we attempt a Key Existence Request "E key"
     * from the node(s) we think are the 3 closest.
     */
    @Override
    public boolean exists(String key) throws Exception {
        List<String> candidates = findClosestNodes(key, 3);

        if (candidates.isEmpty()) {
            return dataMap.containsKey(key);
        }

        for (String cand : candidates) {
            if (cand.equals(myNodeName)) {
                if (dataMap.containsKey(key)) return true;
                continue;
            }

            Boolean remoteExists = sendExistenceRequest(cand, key);
            if (remoteExists != null && remoteExists.booleanValue()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Attempt to read the value from up to 3 closest nodes.
     * If one says "Y <value>", we return that. If all say "N" or "?" we return null.
     */
    @Override
    public String read(String key) throws Exception {
        List<String> candidates = findClosestNodes(key, 3);

        if (candidates.isEmpty()) {
            return dataMap.getOrDefault(key, null);
        }

        for (String cand : candidates) {
            if (cand.equals(myNodeName)) {
                if (dataMap.containsKey(key)) {
                    return dataMap.get(key);
                }
            } else {
                String val = sendReadRequest(cand, key);
                if (val != null) {
                    return val;
                }
            }
        }

        return null;
    }

    /**
     * We'll attempt to store (key,value) in the 3 closest nodes.
     * Return true if at least one node accepted the write (i.e. responded with 'A' or 'R')
     */
    @Override
    public boolean write(String key, String value) throws Exception {
        List<String> candidates = findClosestNodes(key, 3);
        if (candidates.isEmpty()) {
            if (isAmongThreeClosest(key)) {
                dataMap.put(key, value);
                return true;
            }
            return false;
        }

        boolean success = false;
        for (String cand : candidates) {
            if (cand.equals(myNodeName)) {
                if (dataMap.containsKey(key)) {

                }
                dataMap.put(key, value);
                success = true;
            } else {
                Boolean ok = sendWriteRequest(cand, key, value);
                if (ok != null && ok) {
                    success = true;
                }
            }
        }

        return success;
    }

     /**
     * If key == currentValue, replace with newValue at the 3 closest nodes.
     * Return true if at least one node performed the CAS successfully (R or A).
     */
    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        List<String> candidates = findClosestNodes(key, 3);
        if (candidates.isEmpty()) {
            if (isAmongThreeClosest(key)) {
                String existing = dataMap.get(key);
                if ((existing != null) && existing.equals(currentValue)) {
                    dataMap.put(key, newValue);
                    return true;
                } else if (existing == null) {
                    dataMap.put(key, newValue);
                    return true;
                }
            }
            return false;
        }

        boolean success = false;
        for (String cand : candidates) {
            if (cand.equals(myNodeName)) {
                String storedVal = dataMap.get(key);
                if (storedVal != null) {
                    if (storedVal.equals(currentValue)) {
                        dataMap.put(key, newValue);
                        success = true;
                    }
                } else {
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

    /**
     * Parse inbound message, then produce a response if required.
     *
     * The first 2 bytes are TID, then a space, then 1 char request type,
     * then possibly space and other data.
     */
    private void processIncomingMessage(String message, InetAddress senderAddr, int senderPort) {
        try {
            if (message.length() < 4) {
                return;
            }
            String tid = message.substring(0, 2);
            if (message.charAt(2) != ' ') {
                return;
            }
            char mtype = message.charAt(3);

            String remainder = "";
            if (message.length() > 4) {
                remainder = message.substring(4).trim();
            }

            switch(mtype) {
                case 'G':
                    sendResponse(senderAddr, senderPort, tid + " H " + myNodeName);
                    break;

                case 'H':
                    storeResponse(tid, "H " + remainder);
                    break;

                case 'N':
                    processNearestRequest(tid, remainder, senderAddr, senderPort);
                    break;

                case 'O':
                    storeResponse(tid, "O " + remainder);
                    break;

                case 'I':
                    break;

                case 'V':
                    processRelayMessage(tid, remainder, senderAddr, senderPort);
                    break;

                case 'E':
                    processKeyExistRequest(tid, remainder, senderAddr, senderPort);
                    break;

                case 'F':
                    storeResponse(tid, "F " + remainder);
                    break;

                case 'R':
                    processReadRequest(tid, remainder, senderAddr, senderPort);
                    break;

                case 'S':
                    storeResponse(tid, "S " + remainder);
                    break;

                case 'W':
                    processWriteRequest(tid, remainder, senderAddr, senderPort);
                    break;

                case 'X':
                    storeResponse(tid, "X " + remainder);
                    break;

                case 'C':
                    processCASRequest(tid, remainder, senderAddr, senderPort);
                    break;

                case 'D':
                    storeResponse(tid, "D " + remainder);
                    break;

                default:
                    break;
            }

        } catch (Exception e) {
        }
    }

     private void processNearestRequest(String tid, String hashHex, InetAddress senderAddr, int senderPort) throws Exception {
        byte[] targetHash = parseHex(hashHex);
        List<String> best = findClosestNodesByHash(targetHash, 3);

        StringBuilder sb = new StringBuilder();
        for (String nodeName : best) {
            String addr = addressMap.get(nodeName);
            if (addr == null) continue;
            int keySpaces = countSpaces(nodeName);
            int valSpaces = countSpaces(addr);
            sb.append(keySpaces).append(" ").append(nodeName).append(" ")
                    .append(valSpaces).append(" ").append(addr).append(" ");
        }
        String response = tid + " O " + sb.toString();
        sendResponse(senderAddr, senderPort, response);
    }

    /**
     * remainder = "N:TargetNode <innerMessage>"
     * We must forward <innerMessage> to TargetNode, then if it’s a request,
     * we forward the response back to sender using TID of the *relay message*.
     */
    private void processRelayMessage(String tid, String remainder,
                                     InetAddress senderAddr, int senderPort) throws Exception
    {
        int spaceIdx = remainder.indexOf(' ');
        if (spaceIdx < 0) return;
        String targetNode = remainder.substring(0, spaceIdx);
        String innerMsg = remainder.substring(spaceIdx + 1);

        String addr = addressMap.get(targetNode);
        if (addr == null) {
            return;
        }
        String[] parts = addr.split(":");
        if (parts.length != 2) return;
        InetAddress ip = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);

        String innerTID = innerMsg.substring(0, 2);
        RelayInfo rinfo = new RelayInfo(tid, senderAddr, senderPort);
        synchronized (relayPending) {
            relayPending.put(innerTID, rinfo);
        }

        sendMessage(ip, port, innerMsg);
    }

    private Map<String, RelayInfo> relayPending = new HashMap<>();
    private static class RelayInfo {
        public String originalTID;
        public InetAddress senderAddr;
        public int senderPort;
        public RelayInfo(String t, InetAddress a, int p) {
            this.originalTID = t; this.senderAddr = a; this.senderPort = p;
        }
    }

    private void storeResponse(String tid, String data) {
        RelayInfo rinfo = null;
        synchronized (relayPending) {
            rinfo = relayPending.remove(tid);
        }
        if (rinfo != null) {
            try {
                String msg = rinfo.originalTID + " " + data;
                sendResponse(rinfo.senderAddr, rinfo.senderPort, msg);
                return;
            } catch (Exception e) {
            }
        }

        synchronized (responseLock) {
            responseMap.put(tid, data);
        }
    }

    private void processKeyExistRequest(String tid, String key,
                                        InetAddress senderAddr, int senderPort) throws Exception
    {
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

    private void processReadRequest(String tid, String key,
                                    InetAddress senderAddr, int senderPort) throws Exception
    {
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
        int spaces = countSpaces(val);
        String response = tid + " S " + respChar + " " +
                ((respChar=='Y')? (spaces + " " + val + " ") : "0  ");
        sendResponse(senderAddr, senderPort, response);
    }

    private void processWriteRequest(String tid, String remainder,
                                     InetAddress senderAddr, int senderPort) throws Exception
    {
        String[] parts = remainder.split(" ", 4);
        if (parts.length < 4) {
            return;
        }
        String key = parts[1];
        String value = parts[3];

        boolean weHave = dataMap.containsKey(key);
        boolean weAreClose = isAmongThreeClosest(key);

        char respChar;
        if (weHave) {
            dataMap.put(key, value);
            respChar = 'R';
        } else {
            if (weAreClose) {
                dataMap.put(key, value);
                respChar = 'A';
            } else {
                respChar = 'X';
            }
        }

        String response = tid + " X " + respChar;
        sendResponse(senderAddr, senderPort, response);
    }

    private void processCASRequest(String tid, String remainder,
                                   InetAddress senderAddr, int senderPort) throws Exception
    {
        String[] parts = remainder.split(" ", 5);
        if (parts.length < 5) {
            return;
        }

        String key = parts[1];
        String p2 = parts[4];

        String[] sub = p2.split(" ", 2);
        if (sub.length < 2) {
            return;
        }
        String newVal = sub[1];
        String requestedVal = parts[3];

        boolean weHave = dataMap.containsKey(key);
        boolean weAreClose = isAmongThreeClosest(key);
        char respChar;
        if (weHave) {
            String existing = dataMap.get(key);
            if ((existing != null) && existing.equals(requestedVal)) {
                dataMap.put(key, newVal);
                respChar = 'R';
            } else {
                respChar = 'N';
            }
        } else {
            if (weAreClose) {
                dataMap.put(key, newVal);
                respChar = 'A';
            } else {
                respChar = 'X';
            }
        }

        String response = tid + " D " + respChar;
        sendResponse(senderAddr, senderPort, response);
    }

    private void sendMessage(InetAddress ip, int port, String payload) throws IOException {
        byte[] buf = payload.getBytes("UTF-8");
        DatagramPacket packet = new DatagramPacket(buf, buf.length, ip, port);
        socket.send(packet);
    }

    private void sendResponse(InetAddress ip, int port, String response) throws IOException {
        sendMessage(ip, port, response);
    }

    private String generateTID() {
        byte b1 = (byte)(32 + random.nextInt(95));
        byte b2 = (byte)(32 + random.nextInt(95));
        if (b1 == 0x20) b1 = 0x21;
        if (b2 == 0x20) b2 = 0x21;
        return new String(new byte[]{b1, b2});
    }

    /**
     * If our relay stack is non-empty, we must wrap the message inside
     * V <topRelayNode> <msg>, and if more relays exist, that message
     * itself is wrapped, etc.
     */
    private String wrapWithRelay(String tid, String finalTarget, String rawMessage) {
        if (relayStack.isEmpty()) {
            return rawMessage;
        }

        List<String> relays = new ArrayList<>(relayStack);
        String top = relays.get(relays.size() - 1);
        return tid + " V " + top + " " + rawMessage;
    }

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

    private byte[] sha256(String s) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        return md.digest(s.getBytes("UTF-8"));
    }

    /**
     * Distance = 256 - (count of leading bits that match).
     */
    private int distance(byte[] h1, byte[] h2) {
        int count = 0;
        for (int i = 0; i < 32; i++) {
            if (h1[i] == h2[i]) {
                count += 8;
            } else {
                int xor = (h1[i] ^ h2[i]) & 0xFF;
                int bitPos = Integer.numberOfLeadingZeros(xor) - 24;
                count += bitPos;
                break;
            }
        }
        return 256 - count;
    }

    /**
     * Return up to 'maxCount' known nodeNames that are (by our current knowledge)
     * closest to 'key'.
     * We also include ourselves in the set if we have an address for ourselves
     * (which we do).
     */
    private List<String> findClosestNodes(String key, int maxCount) throws Exception {
        byte[] keyHash = sha256(key);
        TreeMap<Integer, List<String>> distMap = new TreeMap<>();
        int dMe = distance(myNodeHashID, keyHash);
        distMap.computeIfAbsent(dMe, k->new ArrayList<>()).add(myNodeName);

        for (String nodeName : addressMap.keySet()) {
            if (!nodeName.startsWith("N:")) continue;
            byte[] hN = sha256(nodeName);
            int d = distance(hN, keyHash);
            distMap.computeIfAbsent(d, k->new ArrayList<>()).add(nodeName);
        }
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

    private List<String> findClosestNodesByHash(byte[] targetHash, int maxCount) throws Exception {
        TreeMap<Integer, List<String>> distMap = new TreeMap<>();
        int dMe = distance(myNodeHashID, targetHash);
        distMap.computeIfAbsent(dMe, k->new ArrayList<>()).add(myNodeName);

        for (String nodeName : addressMap.keySet()) {
            if (!nodeName.startsWith("N:")) continue;
            byte[] hN = sha256(nodeName);
            int d = distance(hN, targetHash);
            distMap.computeIfAbsent(d, k->new ArrayList<>()).add(nodeName);
        }
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

    /**
     * If we have fewer than 3 addresses at a certain distance, we add it.
     * If we have 3, we might prefer to keep older or remove them.
     * (Below we do a simplistic approach that always keeps the earliest 3.)
     */
    private void storeAddressKeyValue(String nodeName, String address) throws Exception {
        addressMap.put(nodeName, address);

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
            }
        }
    }

    private int countSpaces(String s) {
        int c=0;
        for (int i=0; i<s.length(); i++) {
            if (s.charAt(i)==' ') c++;
        }
        return c;
    }

    private Boolean sendExistenceRequest(String nodeName, String key) throws Exception {
        String addr = addressMap.get(nodeName);
        if (addr == null) return null;

        String[] parts = addr.split(":");
        InetAddress ip = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);

        int keySpaces = countSpaces(key);
        String tid = generateTID();
        String msg = tid + " E " + keySpaces + " " + key + " ";

        for (int attempt=0; attempt<3; attempt++) {
            sendMessage(ip, port, wrapWithRelay(tid, nodeName, msg));

            long start = System.currentTimeMillis();
            while(System.currentTimeMillis() - start < 1000) {
                handleIncomingMessages(50);
                String response = null;
                synchronized(responseLock) {
                    response = responseMap.remove(tid);
                }
                if (response != null) {
                    if (!response.startsWith("F ")) return null;
                    char c = response.charAt(2);
                    if (c=='Y') return true;
                    if (c=='N') return false;
                    if (c=='?') return false;
                    return false;
                }
            }
        }
        return null;
    }

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
                    if (!resp.startsWith("S ")) return null;
                    char c = resp.charAt(2);
                    if (c=='Y') {
                        String tail = resp.substring(3).trim();
                        int spaceIndex = tail.indexOf(' ');
                        if (spaceIndex < 0) return "";
                        String val = tail.substring(spaceIndex+1).trim();
                        return val;
                    } else {
                        return null;
                    }
                }
            }
        }
        return null;
    }

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
                        return false;
                    }
                }
            }
        }
        return null;
    }

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
