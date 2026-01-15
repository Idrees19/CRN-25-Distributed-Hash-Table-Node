import java.net.*;
import java.util.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


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

    private String nodeName;
    private DatagramSocket socket;
    private final Map<String, String> kvStore;
    private final Map<Integer,List<String>> addressByDistance;
    private Deque<String> relayStack;

    public Node() {
        this.kvStore = new HashMap<>();
        this.addressByDistance = new HashMap<>();
        this.relayStack = new ArrayDeque<>();
    }

    @Override
    public void setNodeName(String nodeName) throws Exception {
        if (nodeName == null || !nodeName.startsWith("N:")) {
            throw new IllegalArgumentException("Node name must start with 'N:'");
        }
        this.nodeName = nodeName;
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        if (portNumber < 20110 || portNumber > 20130) {
            throw new IllegalArgumentException("Port number out of range (20110-20130)");
        }
        socket = new DatagramSocket(portNumber);
        String ip   = socket.getLocalAddress().getHostAddress();
        int    port = socket.getLocalPort();
        maybeStoreAddress(nodeName, ip + ":" + port);

        socket.setSoTimeout(200);
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        long endTime = (delay == 0) ? Long.MAX_VALUE : (System.currentTimeMillis() + delay);

        while (System.currentTimeMillis() < endTime) {
            try {
                byte[] buf = new byte[1500];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                processIncomingPacket(packet);
            } catch (SocketTimeoutException ste) {
                // no packet arrived in the last 200ms
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }

    private void processIncomingPacket(DatagramPacket packet) throws IOException {
        String received = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
        if (received.length() < 4) {
            return;
        }

        String transactionID = received.substring(0, 2);
        if (received.charAt(2) != ' ') {
            return;
        }
        char msgType = received.charAt(3);

        String payload = "";
        if (received.length() > 5) {
            payload = received.substring(5);
        }

        switch (msgType) {
            case 'W':
                handleWriteRequest(transactionID, payload, packet);
                break;
            case 'R':
                handleReadRequest(transactionID, payload, packet);
                break;
            case 'E':
                handleExistsRequest(transactionID, payload, packet);
                break;
            case 'C':
                handleCasRequest(transactionID, payload, packet);
                break;
            case 'G':
                handleNameRequest(transactionID, packet);
                break;
            case 'N':
                handleNearestRequest(transactionID, payload, packet);
                break;
            case 'V':
                handleRelayRequest(transactionID, payload, packet);
                break;
            case 'I':
                break;
            default:
                break;
        }
    }

    private boolean conditionA(String key) {
        return kvStore.containsKey(key);
    }

    private boolean conditionB(String key) {
        String myHash = computeHash(this.nodeName);
        String keyHash= computeHash(key);
        int myDistance= distance(myHash, keyHash);

        int countStrictlyCloser = 0;
        for (String k : kvStore.keySet()) {
            if (!k.startsWith("N:")) continue;
            String nodeHash = computeHash(k);
            int d = distance(nodeHash, keyHash);
            if (d < myDistance) {
                countStrictlyCloser++;
                if (countStrictlyCloser >= 3) {
                    return false;
                }
            }
        }
        return true;
    }

    private void maybeStoreAddress(String nodeKey, String nodeAddr) {
        String myHash = computeHash(this.nodeName);
        String nodeHash = computeHash(nodeKey);
        int dist = distance(myHash, nodeHash);

        List<String> list = addressByDistance.get(dist);
        if (list == null) {
            list = new ArrayList<>();
            addressByDistance.put(dist, list);
        }
        if (list.size() < 3) {
            list.add(nodeKey);
            kvStore.put(nodeKey, nodeAddr);
        }
        else {
        }
    }

    private void handleWriteRequest(String txID, String payload, DatagramPacket packet) throws IOException {
        try {
            ParseResult pr1 = extractStringWithOffset(payload, 0);
            String key = pr1.value;
            ParseResult pr2 = extractStringWithOffset(payload, pr1.nextOffset);
            String value = pr2.value;

            boolean A = conditionA(key);
            boolean B = conditionB(key);

            char responseChar;
            if (A) {
                kvStore.put(key, value);
                responseChar = 'R';
            } else {
                if (B) {
                    if (key.startsWith("N:")) {
                        maybeStoreAddress(key, value);
                    } else {
                        kvStore.put(key, value);
                    }
                    responseChar = 'A';
                } else {
                    responseChar = 'X';
                }
            }
            String response = txID + " X " + responseChar;
            sendResponse(packet, response);

        } catch (IllegalArgumentException e) {
            sendResponse(packet, txID + " X X");
        }
    }

    private void handleReadRequest(String txID, String payload, DatagramPacket packet) throws IOException {
        try {
            ParseResult pr = extractStringWithOffset(payload, 0);
            String key = pr.value;

            boolean A = conditionA(key);
            boolean B = conditionB(key);

            if (A) {
                String val = kvStore.get(key);
                String encodedVal = encodeString(val);
                String resp = txID + " S Y " + encodedVal;
                sendResponse(packet, resp);
            } else {
                if (B) {
                    String resp = txID + " S N ";
                    sendResponse(packet, resp);
                } else {
                    String resp = txID + " S ? ";
                    sendResponse(packet, resp);
                }
            }
        } catch (IllegalArgumentException e) {
            sendResponse(packet, txID + " S ? ");
        }
    }

    private void handleExistsRequest(String txID, String payload, DatagramPacket packet) throws IOException {
        try {
            ParseResult pr = extractStringWithOffset(payload, 0);
            String key = pr.value;

            boolean A = conditionA(key);
            boolean B = conditionB(key);

            char rc;
            if (A) {
                rc = 'Y';
            } else {
                if (B) rc = 'N';
                else   rc = '?';
            }
            String resp = txID + " F " + rc;
            sendResponse(packet, resp);

        } catch (IllegalArgumentException e) {
            sendResponse(packet, txID + " F ?");
        }
    }

    private void handleCasRequest(String txID, String payload, DatagramPacket packet) throws IOException {
        try {
            ParseResult pr1 = extractStringWithOffset(payload, 0);
            String key = pr1.value;
            ParseResult pr2 = extractStringWithOffset(payload, pr1.nextOffset);
            String requestedValue = pr2.value;
            ParseResult pr3 = extractStringWithOffset(payload, pr2.nextOffset);
            String newValue = pr3.value;

            boolean A = conditionA(key);
            boolean B = conditionB(key);

            char rc;
            if (A) {
                String currentVal = kvStore.get(key);
                if (currentVal.equals(requestedValue)) {
                    kvStore.put(key, newValue);
                    rc = 'R';
                } else {
                    rc = 'N';
                }
            } else {
                if (B) {
                    if (key.startsWith("N:")) {
                        maybeStoreAddress(key, newValue);
                    } else {
                        kvStore.put(key, newValue);
                    }
                    rc = 'A';
                } else {
                    rc = 'X';
                }
            }
            String resp = txID + " D " + rc;
            sendResponse(packet, resp);

        } catch (IllegalArgumentException e) {
            sendResponse(packet, txID + " D X");
        }
    }

    private void handleNameRequest(String txID, DatagramPacket packet) throws IOException {
        String response = txID + " H " + encodeString(nodeName);
        sendResponse(packet, response);
    }

    private void handleNearestRequest(String txID, String payload, DatagramPacket packet) throws IOException {
        String targetHash = payload.trim();

        List<String> nodeKeys = new ArrayList<>();
        for (String k : kvStore.keySet()) {
            if (k.startsWith("N:")) {
                nodeKeys.add(k);
            }
        }
        nodeKeys.sort((a,b) -> {
            int da = distance(computeHash(a), targetHash);
            int db = distance(computeHash(b), targetHash);
            return Integer.compare(da, db);
        });

        StringBuilder sb = new StringBuilder();
        int limit = Math.min(3, nodeKeys.size());
        for (int i = 0; i < limit; i++) {
            String nodeK = nodeKeys.get(i);
            String nodeV = kvStore.get(nodeK);
            sb.append(encodeString(nodeK)).append(" ").append(encodeString(nodeV)).append(" ");
        }
        String response = txID + " O " + sb.toString();
        sendResponse(packet, response);
    }

    private void handleRelayRequest(String txID, String payload, DatagramPacket packet) throws IOException {
        try {
            ParseResult pr1 = extractStringWithOffset(payload, 0);
            String targetNode = pr1.value;

            String embedded = payload.substring(pr1.nextOffset);

            String addr = kvStore.get(targetNode);
            if (addr == null) {
                return;
            }
            String[] parts = addr.split(":");
            InetAddress ip = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            byte[] data = embedded.getBytes(StandardCharsets.UTF_8);
            DatagramPacket out = new DatagramPacket(data, data.length, ip, port);
            socket.send(out);

            byte[] buf = new byte[1500];
            DatagramPacket resp = new DatagramPacket(buf, buf.length);
            socket.receive(resp);
            String respStr = new String(resp.getData(), 0, resp.getLength(), StandardCharsets.UTF_8);

            if (respStr.length() >= 2) {
                String forwarded = txID + respStr.substring(2);
                sendResponse(packet, forwarded);
            }

        } catch (Exception e) {
        }
    }


    @Override
    public boolean isActive(String nodeName) throws Exception {
        String addr = kvStore.get(nodeName);
        if (addr == null) {
            return false;
        }
        String txID = generateTxID();
        String request = txID + " G";
        String response = sendRequestAndWait(addr, request);
        if (response == null) return false;
        if (response.length() < 5) return false;
        if (!response.substring(0,2).equals(txID)) return false;
        if (response.charAt(3) != 'H') return false;
        String gotName = decodeString(response.substring(5));
        return nodeName.equals(gotName);
    }

    @Override
    public void pushRelay(String nodeName) throws Exception {
        if (!nodeName.startsWith("N:")) {
            throw new IllegalArgumentException("Relay nodeName must start with N:");
        }
        if (!kvStore.containsKey(nodeName)) {
            throw new IllegalArgumentException("Unknown node for relay: " + nodeName);
        }
        relayStack.push(nodeName);
    }

    @Override
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            relayStack.pop();
        }
    }

    @Override
    public boolean exists(String key) throws Exception {
        if (kvStore.containsKey(key)) {
            return true;
        }
        List<String> nearest = findClosestNodes(key);
        for (String addr : nearest) {
            String txID = generateTxID();
            String req = txID + " E " + encodeString(key);
            String resp = sendRequestAndWait(addr, req);
            if (resp == null) continue;
            if (resp.startsWith(txID) && resp.length() >= 5 && resp.charAt(3) == 'F') {
                char c = resp.charAt(5);
                if (c == 'Y') {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public String read(String key) throws Exception {
        if (kvStore.containsKey(key)) {
            return kvStore.get(key);
        }
        List<String> nearest = findClosestNodes(key);
        for (String addr : nearest) {
            String txID = generateTxID();
            String req = txID + " R " + encodeString(key);
            String resp = sendRequestAndWait(addr, req);
            if (resp == null) continue;
            if (resp.startsWith(txID) && resp.length() > 5 && resp.charAt(3) == 'S') {
                char c = resp.charAt(5);
                if (c == 'Y') {
                    String valPart = resp.substring(10);
                    return valPart;
                }
            }
        }
        return null;
    }

    @Override
    public boolean write(String key, String value) throws Exception {
        boolean success = false;
        if (key.startsWith("N:")) {
            maybeStoreAddress(key, value);
        } else {
            kvStore.put(key, value);
            success = true;
        }

        List<String> nearest = findClosestNodes(key);
        for (String addr : nearest) {
            String txID = generateTxID();
            String req = txID + " W " + encodeString(key) + " " + encodeString(value);
            String resp = sendRequestAndWait(addr, req);
            if (resp == null) continue;
            if (resp.startsWith(txID) && resp.length() > 5 && resp.charAt(3) == 'X') {
                char c = resp.charAt(5);
                if (c == 'A' || c == 'R') {
                    success = true;
                }
            }
        }
        return success;
    }

    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        if (kvStore.containsKey(key) && kvStore.get(key).equals(currentValue)) {
            kvStore.put(key, newValue);
            return true;
        }
        List<String> nearest = findClosestNodes(key);
        boolean success = false;
        for (String addr : nearest) {
            String txID = generateTxID();
            String req = txID + " C "
                    + encodeString(key) + " "
                    + encodeString(currentValue) + " "
                    + encodeString(newValue);
            String resp = sendRequestAndWait(addr, req);
            if (resp == null) continue;
            if (resp.startsWith(txID) && resp.length() > 5 && resp.charAt(3) == 'D') {
                char c = resp.charAt(5);
                if (c == 'R' || c == 'A') {
                    success = true;
                }
            }
        }
        return success;
    }

    private List<String> findClosestNodes(String key) {
        Set<String> visited = new HashSet<>();
        PriorityQueue<NodeDistance> pq = new PriorityQueue<>(Comparator.comparingInt(nd -> nd.distance));
        for (String k : kvStore.keySet()) {
            if (!k.startsWith("N:")) continue;
            String addr = kvStore.get(k);
            if (addr == null) continue;
            int d = distance(computeHash(k), computeHash(key));
            pq.offer(new NodeDistance(addr, d));
        }

        int expansions = 0;
        while (!pq.isEmpty() && expansions < 100) {
            NodeDistance nd = pq.poll();
            if (visited.contains(nd.addr)) continue;
            visited.add(nd.addr);

            expansions++;
            List<String> discovered = doNearestRequest(nd.addr, computeHash(key));
            for (String newAddr : discovered) {
                if (!visited.contains(newAddr)) {
                    for (Map.Entry<String,String> e : kvStore.entrySet()) {
                        if (e.getValue().equals(newAddr)) {
                            String nodeK = e.getKey();
                            int d2 = distance(computeHash(nodeK), computeHash(key));
                            pq.offer(new NodeDistance(newAddr, d2));
                        }
                    }
                }
            }
        }

        List<NodeDistance> all = new ArrayList<>();
        for (String nodeK : kvStore.keySet()) {
            if (!nodeK.startsWith("N:")) continue;
            String nodeAddr = kvStore.get(nodeK);
            int dist = distance(computeHash(nodeK), computeHash(key));
            all.add(new NodeDistance(nodeAddr, dist));
        }
        all.sort(Comparator.comparingInt(x-> x.distance));
        List<String> result = new ArrayList<>();
        for (int i = 0; i < Math.min(3, all.size()); i++) {
            result.add(all.get(i).addr);
        }
        return result;
    }

    private List<String> doNearestRequest(String addr, String hash) {
        List<String> discovered = new ArrayList<>();
        String txID = generateTxID();
        String req = txID + " N " + hash;
        String resp = sendRequestAndWait(addr, req);
        if (resp == null) {
            return discovered;
        }
        if (resp.startsWith(txID) && resp.length() >= 5 && resp.charAt(3) == 'O') {
            String part = resp.substring(5);
            int offset = 0;
            while (offset < part.length()) {
                try {
                    ParseResult r1 = extractStringWithOffset(part, offset);
                    String discoveredKey = r1.value;
                    offset = r1.nextOffset;
                    ParseResult r2 = extractStringWithOffset(part, offset);
                    String discoveredVal = r2.value;
                    offset = r2.nextOffset;

                    if (discoveredKey.startsWith("N:")) {
                        maybeStoreAddress(discoveredKey, discoveredVal);
                        discovered.add(discoveredVal);
                    }
                } catch (Exception e) {
                    break;
                }
            }
        }
        return discovered;
    }

    private String sendRequestAndWait(String addr, String message) {
        if (addr == null) return null;
        try {
            String[] parts = addr.split(":");
            InetAddress ip = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            String finalMessage = message;
            for (String relayNode : relayStack) {
                finalMessage = generateTxID() + " V " + encodeString(relayNode) + " " + finalMessage;
            }

            for (int attempt = 0; attempt < 3; attempt++) {
                byte[] data = finalMessage.getBytes(StandardCharsets.UTF_8);
                DatagramPacket dp = new DatagramPacket(data, data.length, ip, port);
                socket.send(dp);

                long giveUp = System.currentTimeMillis() + 2000;
                while (System.currentTimeMillis() < giveUp) {
                    try {
                        byte[] buf = new byte[1500];
                        DatagramPacket rec = new DatagramPacket(buf, buf.length);
                        socket.receive(rec);
                        String recStr = new String(rec.getData(), 0, rec.getLength(), StandardCharsets.UTF_8);

                        if (recStr.length() >= 2
                                && recStr.substring(0,2).equals(message.substring(0,2))) {
                            //System.out.println("sendRequestAndWait: got='" + recStr + "'");
                            return recStr;
                        }
                    }
                    catch (SocketTimeoutException ste) {
                    }
                }
            }
        }
        catch (Exception e) {
        }
        return null;
    }

    private void sendResponse(DatagramPacket requestPacket, String response) throws IOException {
        byte[] data = response.getBytes(StandardCharsets.UTF_8);
        DatagramPacket resp = new DatagramPacket(data, data.length,
                requestPacket.getAddress(), requestPacket.getPort());
        socket.send(resp);
    }

    private String computeHash(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        }
        catch (NoSuchAlgorithmException e) {
            return "";
        }
    }

    private int distance(String hashA, String hashB) {
        int matchedBits = 0;
        for (int i = 0; i < 64; i++) {
            char cA = hashA.charAt(i);
            char cB = hashB.charAt(i);
            if (cA == cB) {
                matchedBits += 4;
            } else {
                int nibbleA = Character.digit(cA, 16);
                int nibbleB = Character.digit(cB, 16);
                int xor = nibbleA ^ nibbleB;
                for (int b = 3; b >= 0; b--) {
                    int mask = 1 << b;
                    if ((xor & mask) == 0) {
                        matchedBits++;
                    } else {
                        break;
                    }
                }
                break;
            }
        }
        return 256 - matchedBits;
    }

    private String generateTxID() {
        Random r = new Random();
        char c1 = (char)('A' + r.nextInt(26));
        char c2 = (char)('A' + r.nextInt(26));
        return "" + c1 + c2;
    }

    private static class ParseResult {
        String value;
        int nextOffset;
        ParseResult(String v, int n) {
            value = v; nextOffset = n;
        }
    }

    private ParseResult extractStringWithOffset(String payload, int offset) {
        int spaceCountEnd = payload.indexOf(' ', offset);
        if (spaceCountEnd == -1) {
            throw new IllegalArgumentException("No space after space-count digits in CRN string");
        }
        int spaceCount;
        try {
            spaceCount = Integer.parseInt(payload.substring(offset, spaceCountEnd));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("spaceCount not an integer");
        }
        int stringStart = spaceCountEnd + 1;
        int stringEnd = payload.indexOf(' ', stringStart);
        if (stringEnd == -1) {
            throw new IllegalArgumentException("No trailing space after CRN string");
        }
        String extracted = payload.substring(stringStart, stringEnd);

        int actualSpaces = 0;
        for (char c : extracted.toCharArray()) {
            if (c == ' ') actualSpaces++;
        }
        if (actualSpaces != spaceCount) {
            throw new IllegalArgumentException("CRN string spaceCount mismatch");
        }
        return new ParseResult(extracted, stringEnd + 1);
    }

    private String encodeString(String s) {
        if (s == null) {
            s = "";
        }
        int numSpaces = 0;
        for (char c : s.toCharArray()) {
            if (c == ' ') {
                numSpaces++;
            }
        }
        return numSpaces + " " + s + " ";
    }

    private String decodeString(String chunk) {
        try {
            ParseResult r= extractStringWithOffset(chunk,0);
            return r.value;
        } catch(Exception e){
            return "";
        }
    }

    private static class NodeDistance {
        String addr;
        int distance;
        NodeDistance(String a, int d) {
            addr = a; distance = d;
        }
    }
}
