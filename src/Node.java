// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  YOUR_NAME_GOES_HERE
//  YOUR_STUDENT_ID_NUMBER_GOES_HERE
//  YOUR_EMAIL_GOES_HERE


// DO NOT EDIT starts
// This gives the interface that your code must implement.
// These descriptions are intended to help you understand how the interface
// will be used. See the RFC for how the protocol works.

import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

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

public class Node implements NodeInterface {

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------

    private String nodeName;
    private DatagramSocket socket;

    // Local key/value store
    private Map<String, String> localStore = new HashMap<>();

    // Known peers: nodeName -> "ip:port"
    private Map<String, String> knownNodes = new HashMap<>();

    // Relay stack: bottom = first relay, top = last relay in chain
    private Deque<String> relayStack = new ArrayDeque<>();

    // Transaction ID counter
    private int nextTxID = 1;

    // -----------------------------------------------------------------------
    // Configuration methods
    // -----------------------------------------------------------------------

    public void setNodeName(String nodeName) throws Exception {
        this.nodeName = nodeName;
    }

    public void openPort(int portNumber) throws Exception {
        socket = new DatagramSocket(portNumber);
    }

    // -----------------------------------------------------------------------
    // Network methods
    // -----------------------------------------------------------------------

    public void handleIncomingMessages(int delay) throws Exception {
        if (delay != 0) {
            socket.setSoTimeout(delay);
        } else {
            socket.setSoTimeout(0);
        }

        int peersBefore = knownNodes.size();

        byte[] buffer = new byte[65536];
        while (true) {
            try {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                processMessage(message, packet.getAddress(), packet.getPort());

                // If we just learned about new peers, do a Nearest lookup to discover more
                if (knownNodes.size() > peersBefore) {
                    peersBefore = knownNodes.size();
                    discoverPeers();
                }
            } catch (java.net.SocketTimeoutException e) {
                return;
            }
        }
    }

    // Send Nearest (N) requests to all known peers to discover more nodes
    private void discoverPeers() throws Exception {
        byte[] myHash = HashID.computeHashID(nodeName);
        String myHashHex = bytesToHex(myHash);

        // Ask every known peer for nodes closest to us
        for (Map.Entry<String, String> entry : new HashMap<>(knownNodes).entrySet()) {
            String address = entry.getValue();
            if (address == null) continue;
            String[] parts = address.split(":");
            String ip = parts[0];
            int port = Integer.parseInt(parts[1]);

            String txID = generateTxID();
            // Format: "XX N <hashIDhex>"
            String message = txID + " N " + myHashHex;
            String response = sendAndWait(message, ip, port, txID);

            // Response is handled by processMessage -> handleNearestResponse
            // which automatically adds new peers to knownNodes
        }
    }

    // -----------------------------------------------------------------------
    // isActive: send a Name (G) request and check we get a correct H response
    // -----------------------------------------------------------------------
    public boolean isActive(String targetNodeName) throws Exception {
        String address = knownNodes.get(targetNodeName);
        if (address == null) return false;

        String[] parts = address.split(":");
        String ip = parts[0];
        int port = Integer.parseInt(parts[1]);

        String txID = generateTxID();
        String message = txID + " G ";

        String response = sendAndWait(message, ip, port, txID);
        if (response == null) return false;

        // Response format: "XX H <encodedName>"
        // Check it is an H response and the name matches
        if (response.length() < 4 || response.charAt(3) != 'H') return false;

        String body = response.substring(4).trim();
        int[] r = nextEncodedStringIndices(body);
        if (r == null) return false;
        String receivedName = body.substring(r[0], r[1]);
        return receivedName.equals(targetNodeName);
    }

    // -----------------------------------------------------------------------
    // Relay stack
    // -----------------------------------------------------------------------

    // pushRelay adds to the TOP of the deque (which represents the top of the stack)
    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
    }

    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            relayStack.pop();
        }
    }

    // -----------------------------------------------------------------------
    // Key/value methods
    // -----------------------------------------------------------------------

    // exists: sends Key Existence (E) message, expects F response
    public boolean exists(String key) throws Exception {
        // Check local store first
        if (localStore.containsKey(key)) {
            return true;
        }

        byte[] keyHash = HashID.computeHashID(key);
        String keyHashHex = bytesToHex(keyHash);
        List<String> closest = getClosestNodes(keyHashHex, 3);

        for (String targetNode : closest) {
            String address = knownNodes.get(targetNode);
            if (address == null) continue;

            String[] parts = address.split(":");
            String ip = parts[0];
            int port = Integer.parseInt(parts[1]);

            String txID = generateTxID();
            // Format: "XX E <encodedKey>"
            String message = txID + " E " + encodeString(key);

            String response = sendAndWait(message, ip, port, txID);
            // Response format: "XX F <Y|N|?>"
            if (response != null && response.length() >= 6 && response.charAt(3) == 'F') {
                char code = response.charAt(5);
                if (code == 'Y') return true;
                if (code == 'N') return false;
                // '?' means the node doesn't know — try next node
            }
        }

        return false;
    }

    // read: check local store, then ask closest nodes using R/S messages
    public String read(String key) throws Exception {
        // Check local store first
        if (localStore.containsKey(key)) {
            return localStore.get(key);
        }

        byte[] keyHash = HashID.computeHashID(key);
        String keyHashHex = bytesToHex(keyHash);
        System.out.println("DEBUG read: knownNodes=" + knownNodes.size() + " peers=" + knownNodes.keySet());
        List<String> closest = getClosestNodes(keyHashHex, 3);

        for (String targetNode : closest) {
            String address = knownNodes.get(targetNode);
            if (address == null) continue;

            String[] parts = address.split(":");
            String ip = parts[0];
            int port = Integer.parseInt(parts[1]);

            String txID = generateTxID();
            // Format: "XX R <encodedKey>"
            String message = txID + " R " + encodeString(key);

            String response = sendAndWait(message, ip, port, txID);
            // Response format: "XX S Y <encodedValue>"  or  "XX S N"  or  "XX S ?"
            if (response != null && response.length() >= 6 && response.charAt(3) == 'S') {
                char code = response.charAt(5);
                if (code == 'Y' && response.length() > 7) {
                    // Decode the value that follows "XX S Y "
                    String valueBody = response.substring(7);
                    return decodeValue(valueBody);
                }
                if (code == 'N') return null;
                // '?' means unknown — try next node
            }
        }

        return null;
    }

    // write: store locally, send W messages to the 3 closest nodes
    public boolean write(String key, String value) throws Exception {
        // Store locally
        localStore.put(key, value);

        byte[] keyHash = HashID.computeHashID(key);
        String keyHashHex = bytesToHex(keyHash);
        List<String> closest = getClosestNodes(keyHashHex, 3);

        // If no peers known yet, local store is the best we can do — report success
        if (closest.isEmpty()) return true;

        boolean anySuccess = false;

        for (String targetNode : closest) {
            String address = knownNodes.get(targetNode);
            if (address == null) continue;

            String[] parts = address.split(":");
            String ip = parts[0];
            int port = Integer.parseInt(parts[1]);

            String txID = generateTxID();
            // Format: "XX W <encodedKey><encodedValue>"
            String message = txID + " W " + encodeString(key) + encodeString(value);

            String response = sendAndWait(message, ip, port, txID);
            // Response format: "XX X <A|R|X>"
            // A = added (new), R = replaced (updated), X = rejected
            if (response != null && response.length() >= 6 && response.charAt(3) == 'X') {
                char code = response.charAt(5);
                if (code == 'A' || code == 'R') {
                    anySuccess = true;
                }
            }
        }

        return anySuccess;
    }

    // CAS: atomically compare-and-swap a value on the network
    // Sends C message, expects D response
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        byte[] keyHash = HashID.computeHashID(key);
        String keyHashHex = bytesToHex(keyHash);
        List<String> closest = getClosestNodes(keyHashHex, 3);

        for (String targetNode : closest) {
            String address = knownNodes.get(targetNode);
            if (address == null) continue;

            String[] parts = address.split(":");
            String ip = parts[0];
            int port = Integer.parseInt(parts[1]);

            String txID = generateTxID();
            // Format: "XX C <encodedKey><encodedCurrentValue><encodedNewValue>"
            String message = txID + " C " + encodeString(key)
                    + encodeString(currentValue)
                    + encodeString(newValue);

            String response = sendAndWait(message, ip, port, txID);
            // Response format: "XX D <Y|N>"
            // Y = swap succeeded, N = current value didn't match (or key missing)
            if (response != null && response.length() >= 6 && response.charAt(3) == 'D') {
                char code = response.charAt(5);
                if (code == 'Y') {
                    // Update local store to reflect the swap
                    localStore.put(key, newValue);
                    return true;
                }
                if (code == 'N') {
                    return false;
                }
            }
        }

        return false;
    }

    // -----------------------------------------------------------------------
    // Message processing (incoming requests from other nodes)
    // -----------------------------------------------------------------------

    private void processMessage(String message, InetAddress senderAddress, int senderPort) {
        if (message == null || message.length() < 4) return;

        String txID = message.substring(0, 2);
        char type = message.charAt(3);

        try {
            switch (type) {
                case 'G': handleNameRequest(txID, senderAddress, senderPort); break;
                case 'H': handleNameResponse(message, senderAddress, senderPort); break;
                case 'N': handleNearestRequest(txID, message, senderAddress, senderPort); break;
                case 'O': handleNearestResponse(message, senderAddress, senderPort); break;
                case 'E': handleExistsRequest(txID, message, senderAddress, senderPort); break;
                case 'R': handleReadRequest(txID, message, senderAddress, senderPort); break;
                case 'W': handleWriteRequest(txID, message, senderAddress, senderPort); break;
                case 'C': handleCASRequest(txID, message, senderAddress, senderPort); break;
                case 'V': handleRelayRequest(txID, message, senderAddress, senderPort); break;
                // Ignore response types and unknown types silently
                default: break;
            }
        } catch (Exception e) {
            // Swallow errors so one bad message doesn't crash the node
        }
    }

    // G: Name request — reply with our node name
    private void handleNameRequest(String txID, InetAddress senderAddress, int senderPort) throws Exception {
        // Format: "XX H <encodedName>"
        String response = txID + " H " + encodeString(nodeName);
        sendMessage(response, senderAddress, senderPort);
    }

    // H: Name response — learn the sender's name
    private void handleNameResponse(String message, InetAddress senderAddress, int senderPort) {
        String body = message.substring(4).trim();
        int[] r = nextEncodedStringIndices(body);
        if (r == null) return;
        String name = body.substring(r[0], r[1]);
        if (!name.equals(nodeName)) {
            knownNodes.put(name, senderAddress.getHostAddress() + ":" + senderPort);
        }
    }

    // N: Nearest request — return the 3 closest nodes we know to the given hashID
    private void handleNearestRequest(String txID, String message, InetAddress senderAddress, int senderPort) throws Exception {
        // Format: "XX N <hashIDhex>"
        String hashID = message.substring(4).trim();

        List<String> closest = getClosestNodes(hashID, 3);

        // Format: "XX O <encodedName1><encodedAddr1>..."
        StringBuilder response = new StringBuilder(txID + " O ");
        for (String name : closest) {
            String address = knownNodes.get(name);
            if (address != null) {
                response.append(encodeString(name));
                response.append(encodeString(address));
            }
        }

        sendMessage(response.toString(), senderAddress, senderPort);
    }

    // O: Nearest response — learn about new nodes
    private void handleNearestResponse(String message, InetAddress senderAddress, int senderPort) {
        String body = message.substring(4).trim();

        while (!body.isEmpty()) {
            int[] r1 = nextEncodedStringIndices(body);
            if (r1 == null) break;
            String name = body.substring(r1[0], r1[1]);
            body = body.substring(r1[1]).trim();

            int[] r2 = nextEncodedStringIndices(body);
            if (r2 == null) break;
            String address = body.substring(r2[0], r2[1]);
            body = body.substring(r2[1]).trim();

            if (!name.equals(nodeName)) {
                knownNodes.put(name, address);
            }
        }
    }

    // E: Key Existence request — reply Y/N/? for whether key is stored here
    private void handleExistsRequest(String txID, String message, InetAddress senderAddress, int senderPort) throws Exception {
        String body = message.substring(4).trim();
        int[] r = nextEncodedStringIndices(body);
        if (r == null) return;
        String key = body.substring(r[0], r[1]);

        // We only answer definitively if we store it; otherwise '?'
        char code = localStore.containsKey(key) ? 'Y' : '?';
        // Format: "XX F <Y|N|?>"
        String response = txID + " F " + code;
        sendMessage(response, senderAddress, senderPort);
    }

    // R: Read request — reply with value if we have it
    private void handleReadRequest(String txID, String message, InetAddress senderAddress, int senderPort) throws Exception {
        String body = message.substring(4).trim();
        int[] r = nextEncodedStringIndices(body);
        if (r == null) return;
        String key = body.substring(r[0], r[1]);

        String response;
        if (localStore.containsKey(key)) {
            // Format: "XX S Y <encodedValue>"
            response = txID + " S Y " + encodeString(localStore.get(key));
        } else {
            // Format: "XX S N"
            response = txID + " S N";
        }
        sendMessage(response, senderAddress, senderPort);
    }

    // W: Write request — store and reply A (added), R (replaced), or X (rejected)
    private void handleWriteRequest(String txID, String message, InetAddress senderAddress, int senderPort) throws Exception {
        String body = message.substring(4).trim();

        int[] r1 = nextEncodedStringIndices(body);
        if (r1 == null) return;
        String key = body.substring(r1[0], r1[1]);
        body = body.substring(r1[1]).trim();

        int[] r2 = nextEncodedStringIndices(body);
        if (r2 == null) return;
        String value = body.substring(r2[0], r2[1]);

        char code;
        if (localStore.containsKey(key)) {
            code = 'R'; // replacing existing value
        } else {
            code = 'A'; // adding new value
        }
        localStore.put(key, value);

        // Format: "XX X <A|R|X>"
        String response = txID + " X " + code;
        sendMessage(response, senderAddress, senderPort);
    }

    // C: Compare-and-Swap request — atomically swap value if current matches
    private void handleCASRequest(String txID, String message, InetAddress senderAddress, int senderPort) throws Exception {
        String body = message.substring(4).trim();

        int[] r1 = nextEncodedStringIndices(body);
        if (r1 == null) return;
        String key = body.substring(r1[0], r1[1]);
        body = body.substring(r1[1]).trim();

        int[] r2 = nextEncodedStringIndices(body);
        if (r2 == null) return;
        String currentValue = body.substring(r2[0], r2[1]);
        body = body.substring(r2[1]).trim();

        int[] r3 = nextEncodedStringIndices(body);
        if (r3 == null) return;
        String newValue = body.substring(r3[0], r3[1]);

        // Atomically compare and swap
        char code;
        String stored = localStore.get(key);
        if (stored != null && stored.equals(currentValue)) {
            localStore.put(key, newValue);
            code = 'Y'; // success
        } else {
            code = 'N'; // mismatch or key not found
        }

        // Format: "XX D <Y|N>"
        String response = txID + " D " + code;
        sendMessage(response, senderAddress, senderPort);
    }

    // V: Relay request — forward the embedded message to the next hop and relay back the response
    private void handleRelayRequest(String txID, String message, InetAddress senderAddress, int senderPort) throws Exception {
        // Format: "XX V <encodedTargetName><encodedTargetAddress><innerMessage>"
        String body = message.substring(4).trim();

        // Decode target node name
        int[] r1 = nextEncodedStringIndices(body);
        if (r1 == null) return;
        String targetName = body.substring(r1[0], r1[1]);
        body = body.substring(r1[1]).trim();

        // Decode target address
        int[] r2 = nextEncodedStringIndices(body);
        if (r2 == null) return;
        String targetAddress = body.substring(r2[0], r2[1]);
        body = body.substring(r2[1]).trim();

        // body is now the inner message to forward
        String innerMessage = body;
        if (innerMessage.length() < 4) return;

        // Extract the inner txID (first 2 chars of inner message)
        String innerTxID = innerMessage.substring(0, 2);

        // Generate a new txID for our forwarded copy so we can match the response
        String forwardTxID = generateTxID();

        // Rewrite the inner message with our new txID
        String forwardedMessage = forwardTxID + innerMessage.substring(2);

        // Parse the target address
        String[] parts = targetAddress.split(":");
        String targetIP = parts[0];
        int targetPort = Integer.parseInt(parts[1]);

        // Forward the message and wait for a response
        String innerResponse = sendAndWait(forwardedMessage, targetIP, targetPort, forwardTxID);

        if (innerResponse != null) {
            // Rewrite the response txID back to the original inner txID
            String rewrittenResponse = innerTxID + innerResponse.substring(2);

            // Send the relay response back to the original sender
            // Format: "XX V <rewrittenInnerResponse>"
            String relayResponse = txID + " V " + rewrittenResponse;
            sendMessage(relayResponse, senderAddress, senderPort);
        }
    }

    // -----------------------------------------------------------------------
    // sendAndWait: send a UDP message and wait for a matching txID response
    // Retries up to 3 times with a 5-second timeout each attempt (per RFC)
    // While waiting, processes any other messages that arrive
    // -----------------------------------------------------------------------
    private String sendAndWait(String message, String ip, int port, String txID) throws Exception {
        InetAddress address = InetAddress.getByName(ip);
        return sendAndWait(message, address, port, txID);
    }

    private String sendAndWait(String message, InetAddress address, int port, String txID) throws Exception {
        for (int attempt = 0; attempt < 3; attempt++) {
            sendMessage(message, address, port);

            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline) {
                int remaining = (int)(deadline - System.currentTimeMillis());
                if (remaining <= 0) break;

                socket.setSoTimeout(remaining);
                try {
                    byte[] buffer = new byte[65536];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);
                    String response = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);

                    // Always process the message so we handle other node requests
                    processMessage(response, packet.getAddress(), packet.getPort());

                    // Return if this is the response we are waiting for
                    if (response.startsWith(txID)) {
                        return response;
                    }

                } catch (java.net.SocketTimeoutException e) {
                    break; // timeout — retry
                }
            }
        }
        return null; // no response after 3 attempts
    }

    private void sendMessage(String message, InetAddress address, int port) throws Exception {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
        socket.send(packet);
    }

    // -----------------------------------------------------------------------
    // String encoding/decoding (CRN format: "<numSpaces> <content> ")
    // -----------------------------------------------------------------------

    // Encodes a string: "Hello World" -> "1 Hello World "
    private String encodeString(String s) {
        long spaces = s.chars().filter(c -> c == ' ').count();
        return spaces + " " + s + " ";
    }

    // Decodes a full encoded string field: "1 Hello World " -> "Hello World"
    // Used when the encoded string is the only/last token in a field
    private String decodeValue(String s) {
        int firstSpace = s.indexOf(' ');
        if (firstSpace == -1) return s;
        String content = s.substring(firstSpace + 1);
        // Strip the trailing space that the encoding adds
        if (content.endsWith(" ")) {
            content = content.substring(0, content.length() - 1);
        }
        return content;
    }

    // Returns [contentStart, contentEnd] within the (un-trimmed) string s
    // for the next CRN-encoded token, or null if malformed.
    // After calling this, advance by result[1] + 1 to skip the trailing space.
    private int[] nextEncodedStringIndices(String s) {
        // Find the space after the count
        int firstSpace = s.indexOf(' ');
        if (firstSpace == -1) return null;

        int numSpaces;
        try {
            numSpaces = Integer.parseInt(s.substring(0, firstSpace));
        } catch (NumberFormatException e) {
            return null;
        }

        int contentStart = firstSpace + 1;
        int spacesSeen = 0;

        for (int i = contentStart; i < s.length(); i++) {
            if (s.charAt(i) == ' ') {
                if (spacesSeen == numSpaces) {
                    // i is the index of the terminating space
                    return new int[]{contentStart, i};
                }
                spacesSeen++;
            }
        }
        return null; // not enough spaces found
    }

    // -----------------------------------------------------------------------
    // Node lookup helpers
    // -----------------------------------------------------------------------

    // Returns up to `count` known node names sorted by distance to targetHashIDHex
    private List<String> getClosestNodes(String targetHashIDHex, int count) throws Exception {
        final byte[] targetHash;
        // targetHashIDHex may be a raw hex string (from N messages) or a node name
        // If it looks like a 64-char hex string treat it as a hash, otherwise hash it
        if (targetHashIDHex.matches("[0-9a-fA-F]{64}")) {
            targetHash = hexToBytes(targetHashIDHex);
        } else {
            targetHash = HashID.computeHashID(targetHashIDHex);
        }

        List<String> sorted = new ArrayList<>(knownNodes.keySet());
        sorted.sort((a, b) -> {
            try {
                byte[] hashA = HashID.computeHashID(a);
                byte[] hashB = HashID.computeHashID(b);
                return Integer.compare(distance(hashA, targetHash), distance(hashB, targetHash));
            } catch (Exception e) {
                return 0;
            }
        });

        return sorted.subList(0, Math.min(count, sorted.size()));
    }

    // -----------------------------------------------------------------------
    // Transaction ID generation
    // Produces 2-char IDs using printable non-space ASCII (33..125)
    // -----------------------------------------------------------------------
    private String generateTxID() {
        int id = nextTxID++;
        // Wrap around after 93*93 = 8649 IDs
        int range = 93;
        char c1 = (char)(33 + (id / range) % range);
        char c2 = (char)(33 + (id % range));
        return "" + c1 + c2;
    }

    // -----------------------------------------------------------------------
    // Distance: 256 minus the number of matching leading bits
    // -----------------------------------------------------------------------
    private int distance(byte[] hash1, byte[] hash2) {
        int matchingBits = 0;
        for (int i = 0; i < hash1.length; i++) {
            int xor = (hash1[i] & 0xFF) ^ (hash2[i] & 0xFF);
            if (xor == 0) {
                matchingBits += 8;
            } else {
                matchingBits += Integer.numberOfLeadingZeros(xor) - 24;
                break;
            }
        }
        return 256 - matchingBits;
    }

    // -----------------------------------------------------------------------
    // Hex utilities
    // -----------------------------------------------------------------------

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte)((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }
}