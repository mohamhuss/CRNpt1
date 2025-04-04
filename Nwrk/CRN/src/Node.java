import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

// **NodeInterface**: Defines the contract for node operations in the DHT network
interface NodeInterface {
    void setNodeName(String nodeName) throws Exception;
    void openPort(int portNumber) throws Exception;
    void handleIncomingMessages(int delay) throws Exception;
    boolean isActive(String nodeName) throws Exception;
    void pushRelay(String nodeName) throws Exception;
    void popRelay() throws Exception;
    boolean exists(String key) throws Exception;
    String read(String key) throws Exception;
    boolean write(String key, String value) throws Exception;
    boolean CAS(String key, String currentValue, String newValue) throws Exception;
}

// **Node**: Implements the NodeInterface for a DHT node with heavy debugging
public class Node implements NodeInterface {
    // **Debugging Flag**: Set to true for detailed logs, false to disable
    private static final boolean DEBUG_MODE = true;

    // Node identification
    private String nodeName;
    private byte[] nodeHashID;

    // Network communication
    private DatagramSocket socket;
    private int port;

    // Storage for key-value pairs
    private Map<String, String> keyValueStore = new ConcurrentHashMap<>();

    // Maps distance -> list of address key-value pairs
    private Map<Integer, List<AddressKeyValuePair>> addressKeyValuesByDistance = new ConcurrentHashMap<>();

    // Stack for relay nodes
    private Stack<String> relayStack = new Stack<>();

    // Transaction tracking
    private Map<String, ResponseCallback> pendingTransactions = new ConcurrentHashMap<>();
    private AtomicInteger txidCounter = new AtomicInteger(0);

    // For handling timeouts and retries
    private ExecutorService executor = Executors.newCachedThreadPool();
    private Thread listenerThread;

    // Constants
    private static final int MAX_PACKET_SIZE = 1024;
    private static final int REQUEST_TIMEOUT = 3000; // ms
    private static final int MAX_RETRIES = 3;

    // Random number generator for transaction IDs
    private Random random = new Random();

    // Inner class for address key-value pairs with distance calculation
    private class AddressKeyValuePair {
        String key;
        String value;
        byte[] hashID;
        int distance;

        AddressKeyValuePair(String key, String value) throws Exception {
            this.key = key;
            this.value = value;
            this.hashID = HashID.computeHashID(key);
            this.distance = calculateDistance(nodeHashID, this.hashID);
            debugLog("Created AddressKeyValuePair for key: " + key + ", distance: " + distance);
        }
    }

    // Callback interface for handling asynchronous responses
    private interface ResponseCallback {
        void onResponse(String response);
        void onTimeout();
    }

    // Helper class for node addresses
    private class NodeAddress {
        String name;
        InetAddress address;
        int port;

        NodeAddress(String name, InetAddress address, int port) {
            this.name = name;
            this.address = address;
            this.port = port;
            debugLog("Created NodeAddress for node: " + name + " at " + address + ":" + port);
        }
    }

    // **Utility Class for HashID computation**
    private static class HashID {
        static byte[] computeHashID(String input) throws Exception {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            debugLog("Computed hash for input: " + input + ", hash: " + bytesToHex(hash));
            return hash;
        }
    }

    // **Centralized Debug Logging Method**
    private static void debugLog(String message) {
        if (DEBUG_MODE) {
            System.out.println("[DEBUG] " + message);
        }
    }

    // **Centralized Error Logging Method**
    private void errorLog(String message) {
        System.err.println("[ERROR] " + message);
    }

    // **Set the node's name and compute its hash ID**
    @Override
    public void setNodeName(String nodeName) throws Exception {
        debugLog("Setting node name to: " + nodeName);
        if (!nodeName.startsWith("N:")) {
            errorLog("Invalid node name: " + nodeName + " - must start with 'N:'");
            throw new IllegalArgumentException("Node name must start with N:");
        }
        this.nodeName = nodeName;
        this.nodeHashID = HashID.computeHashID(nodeName);
        debugLog("Node hash ID computed: " + bytesToHex(this.nodeHashID));
    }

    // **Open a UDP port for communication**
    @Override
    public void openPort(int portNumber) throws Exception {
        debugLog("Opening port: " + portNumber);
        this.port = portNumber;

        try {
            this.socket = new DatagramSocket(portNumber);
            debugLog("Successfully bound to port: " + portNumber);
        } catch (BindException e) {
            debugLog("BindException on port " + portNumber + ": " + e.getMessage());
            boolean success = false;
            for (int offset = 1; offset <= 5; offset++) {
                try {
                    this.socket = new DatagramSocket(portNumber + offset);
                    this.port = portNumber + offset;
                    debugLog("Successfully bound to alternative port: " + this.port);
                    success = true;
                    break;
                } catch (BindException be) {
                    debugLog("Failed to bind to port " + (portNumber + offset));
                }
            }
            if (!success) {
                errorLog("Could not find an available port. Tried ports " + portNumber + " through " + (portNumber + 5));
                throw new Exception("Could not find an available port.");
            }
        }

        String localIP = InetAddress.getLocalHost().getHostAddress();
        String value = localIP + ":" + this.port;
        keyValueStore.put(nodeName, value);
        debugLog("Stored self address: " + nodeName + " -> " + value);

        AddressKeyValuePair selfPair = new AddressKeyValuePair(nodeName, value);
        addAddressKeyValuePair(selfPair);

        startListenerThread();
    }

    // **Start a background thread to listen for incoming messages**
    private void startListenerThread() {
        debugLog("Starting listener thread");
        listenerThread = new Thread(() -> {
            try {
                while (!socket.isClosed()) {
                    byte[] buffer = new byte[MAX_PACKET_SIZE];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    debugLog("Waiting for incoming packet...");
                    socket.receive(packet);
                    String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                    debugLog("Received message: '" + message + "' from " + packet.getAddress() + ":" + packet.getPort());
                    executor.submit(() -> {
                        try {
                            handleBootstrapMessage(message, packet.getAddress(), packet.getPort());
                            processMessage(packet);
                        } catch (Exception e) {
                            errorLog("Error handling message: " + e.getMessage());
                        }
                    });
                }
            } catch (IOException e) {
                if (!socket.isClosed()) {
                    errorLog("Socket error in listener: " + e.getMessage());
                }
            }
        });
        listenerThread.setDaemon(true);
        listenerThread.start();
        debugLog("Listener thread started");
    }

    // **Handle bootstrap messages to learn about other nodes**
    private void handleBootstrapMessage(String message, InetAddress sourceAddress, int sourcePort) {
        debugLog("Handling bootstrap message: " + message);
        if (message.contains("W") && message.contains("N:")) {
            String[] parts = message.split(" ");
            if (parts.length >= 6 && parts[1].equals("W")) {
                try {
                    int keySpaceCount = Integer.parseInt(parts[2]);
                    String key = parts[3];
                    if (!key.startsWith("N:")) {
                        debugLog("Invalid node name in bootstrap: " + key);
                        return;
                    }

                    int valueSpaceCount = Integer.parseInt(parts[4]);
                    String value = parts[5];
                    if (!value.contains(":")) {
                        debugLog("Invalid address format in bootstrap: " + value);
                        return;
                    }
                    String[] addrParts = value.split(":");
                    if (addrParts.length != 2) {
                        debugLog("Invalid address parts in bootstrap: " + value);
                        return;
                    }
                    String ip = addrParts[0];
                    int port = Integer.parseInt(addrParts[1]);

                    InetAddress addr = InetAddress.getByName(ip);
                    NodeAddress nodeAddr = new NodeAddress(key, addr, port);
                    addNodeAddress(key, nodeAddr);
                    keyValueStore.put(key, value);
                    debugLog("Added node from bootstrap: " + key + " -> " + value);
                } catch (Exception e) {
                    errorLog("Failed to parse bootstrap message: " + message + " - " + e.getMessage());
                }
            } else {
                debugLog("Bootstrap message format invalid: " + message);
            }
        } else {
            debugLog("Ignored non-bootstrap message: " + message);
        }
    }

    // **Add a node address to the routing table**
    private void addNodeAddress(String nodeName, NodeAddress nodeAddr) {
        try {
            AddressKeyValuePair pair = new AddressKeyValuePair(nodeName,
                    nodeAddr.address.getHostAddress() + ":" + nodeAddr.port);
            addAddressKeyValuePair(pair);
            debugLog("Added node address: " + nodeName + " -> " + nodeAddr.address + ":" + nodeAddr.port);
        } catch (Exception e) {
            errorLog("Failed to add node address: " + e.getMessage());
        }
    }

    // **Add an address key-value pair to the routing table**
    private void addAddressKeyValuePair(AddressKeyValuePair pair) {
        keyValueStore.put(pair.key, pair.value);
        debugLog("Stored key-value pair: " + pair.key + " -> " + pair.value);

        List<AddressKeyValuePair> pairsAtDistance = addressKeyValuesByDistance.computeIfAbsent(
                pair.distance, k -> new CopyOnWriteArrayList<>());

        boolean exists = false;
        for (int i = 0; i < pairsAtDistance.size(); i++) {
            if (pairsAtDistance.get(i).key.equals(pair.key)) {
                pairsAtDistance.set(i, pair);
                exists = true;
                debugLog("Updated existing node in routing table: " + pair.key);
                break;
            }
        }

        if (!exists) {
            pairsAtDistance.add(pair);
            debugLog("Added new node to routing table: " + pair.key + " at distance " + pair.distance);
            if (pairsAtDistance.size() > 3) {
                pairsAtDistance.remove(pairsAtDistance.size() - 1);
                debugLog("Removed least recent node from distance " + pair.distance);
            }
        }
    }

    // **Handle incoming messages with a specified delay**
    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        debugLog("Handling incoming messages with delay: " + delay + " ms");
        if (socket == null) {
            errorLog("Socket not initialized. Call openPort first.");
            throw new IllegalStateException("Socket not initialized.");
        }

        if (delay <= 0) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return;
        }

        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // **Process an incoming UDP message**
    private void processMessage(DatagramPacket packet) {
        String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
        debugLog("Processing message: " + message);
        if (message.length() < 4) {
            debugLog("Message too short: " + message);
            return;
        }

        try {
            String txid = message.substring(0, 2);
            char messageType = message.charAt(3);
            debugLog("Parsed TXID: " + txid + ", Message Type: " + messageType);

            ResponseCallback callback = pendingTransactions.remove(txid);
            if (callback != null) {
                debugLog("Found pending transaction for TXID: " + txid);
                callback.onResponse(message);
                return;
            }

            switch (messageType) {
                case 'G': // Name request
                    debugLog("Handling name request");
                    handleNameRequest(txid, packet.getAddress(), packet.getPort());
                    break;
                case 'N': // Nearest request
                    if (message.length() > 5) {
                        debugLog("Handling nearest request");
                        handleNearestRequest(txid, message.substring(5), packet.getAddress(), packet.getPort());
                    }
                    break;
                case 'E': // Key existence request
                    if (message.length() > 5) {
                        debugLog("Handling key existence request");
                        handleKeyExistenceRequest(txid, message.substring(5), packet.getAddress(), packet.getPort());
                    }
                    break;
                case 'R': // Read request
                    if (message.length() > 5) {
                        debugLog("Handling read request");
                        handleReadRequest(txid, message.substring(5), packet.getAddress(), packet.getPort());
                    }
                    break;
                case 'W': // Write request
                    if (message.length() > 5) {
                        debugLog("Handling write request");
                        handleWriteRequest(txid, message.substring(5), packet.getAddress(), packet.getPort());
                    }
                    break;
                case 'C': // Compare and swap request
                    if (message.length() > 5) {
                        debugLog("Handling CAS request");
                        handleCASRequest(txid, message.substring(5), packet.getAddress(), packet.getPort());
                    }
                    break;
                case 'V': // Relay request
                    if (message.length() > 5) {
                        debugLog("Handling relay request");
                        handleRelayRequest(txid, message.substring(5), packet.getAddress(), packet.getPort());
                    }
                    break;
                case 'I': // Information message
                    debugLog("Received information message: " + message);
                    break;
                default:
                    debugLog("Unknown message type: " + messageType);
            }
        } catch (Exception e) {
            errorLog("Exception in processMessage: " + e.getMessage());
        }
    }

    // **Handle a name request**
    private void handleNameRequest(String txid, InetAddress address, int port) throws Exception {
        debugLog("Handling name request for TXID: " + txid);
        String response = txid + " H " + formatString(nodeName);
        sendPacket(response, address, port);
        debugLog("Sent name response: " + response);
    }

    // **Handle a nearest node request**
    private void handleNearestRequest(String txid, String hashIDHex, InetAddress address, int port) throws Exception {
        debugLog("Handling nearest request for TXID: " + txid + ", hashIDHex: " + hashIDHex);
        try {
            byte[] targetHashID = hexStringToByteArray(hashIDHex.trim());
            List<AddressKeyValuePair> nearestNodes = findNearestNodes(targetHashID, 3);

            StringBuilder response = new StringBuilder(txid + " O ");
            for (AddressKeyValuePair pair : nearestNodes) {
                response.append(formatKeyValuePair(pair.key, pair.value));
            }
            sendPacket(response.toString(), address, port);
            debugLog("Sent nearest nodes response: " + response.toString());
        } catch (Exception e) {
            errorLog("Failed to handle nearest request: " + e.getMessage());
        }
    }

    // **Handle a key existence request**
    private void handleKeyExistenceRequest(String txid, String keyString, InetAddress address, int port) throws Exception {
        debugLog("Handling key existence request for TXID: " + txid + ", keyString: " + keyString);
        String key = parseString(keyString);
        if (key == null) {
            debugLog("Failed to parse key from keyString: " + keyString);
            return;
        }
        char responseChar;
        if (keyValueStore.containsKey(key)) {
            responseChar = 'Y';
        } else {
            byte[] keyHashID = HashID.computeHashID(key);
            responseChar = shouldStoreKey(keyHashID) ? 'N' : '?';
        }
        String response = txid + " F " + responseChar;
        sendPacket(response, address, port);
        debugLog("Sent key existence response: " + response);
    }

    // **Handle a read request**
    private void handleReadRequest(String txid, String keyString, InetAddress address, int port) throws Exception {
        debugLog("Handling read request for TXID: " + txid + ", keyString: " + keyString);
        String key = parseString(keyString);
        if (key == null) {
            debugLog("Failed to parse key from keyString: " + keyString);
            return;
        }
        char responseChar;
        String value = "";
        if (keyValueStore.containsKey(key)) {
            responseChar = 'Y';
            value = keyValueStore.get(key);
        } else {
            byte[] keyHashID = HashID.computeHashID(key);
            responseChar = shouldStoreKey(keyHashID) ? 'N' : '?';
        }
        String response = txid + " S " + responseChar + " " + formatString(value);
        sendPacket(response, address, port);
        debugLog("Sent read response: " + response);
    }

    // **Handle a write request**
    private void handleWriteRequest(String txid, String message, InetAddress address, int port) throws Exception {
        debugLog("Handling write request for TXID: " + txid + ", message: " + message);
        String[] parts = message.split(" ", 3);
        if (parts.length < 3) {
            debugLog("Invalid write request format: " + message);
            return;
        }
        int keySpaceCount = Integer.parseInt(parts[0]);
        String key = parts[1];
        if (countSpaces(key) != keySpaceCount) {
            debugLog("Space count mismatch for key: " + key);
            return;
        }
        String[] valueParts = parts[2].split(" ", 2);
        if (valueParts.length < 2) {
            debugLog("Invalid value format in write request: " + parts[2]);
            return;
        }
        int valueSpaceCount = Integer.parseInt(valueParts[0]);
        String value = valueParts[1];
        if (countSpaces(value) != valueSpaceCount) {
            debugLog("Space count mismatch for value: " + value);
            return;
        }

        char responseChar;
        if (key.startsWith("N:") && !key.equals(nodeName)) {
            try {
                String[] addrParts = value.split(":");
                if (addrParts.length == 2) {
                    String ip = addrParts[0];
                    int nodePort = Integer.parseInt(addrParts[1]);
                    AddressKeyValuePair pair = new AddressKeyValuePair(key, value);
                    addAddressKeyValuePair(pair);
                    debugLog("Added node from write request: " + key + " -> " + value);
                }
            } catch (Exception e) {
                errorLog("Failed to add node from write request: " + e.getMessage());
            }
        }

        if (keyValueStore.containsKey(key)) {
            keyValueStore.put(key, value);
            responseChar = 'R';
            debugLog("Updated existing key: " + key + " -> " + value);
        } else if (key.startsWith("D:") || address.isLoopbackAddress()) {
            keyValueStore.put(key, value);
            responseChar = 'A';
            debugLog("Added new key: " + key + " -> " + value);
        } else {
            byte[] keyHashID = HashID.computeHashID(key);
            responseChar = shouldStoreKey(keyHashID) ? 'A' : 'X';
            if (responseChar == 'A') {
                keyValueStore.put(key, value);
                debugLog("Accepted new key: " + key + " -> " + value);
            } else {
                debugLog("Rejected write for key: " + key + " (not in storage range)");
            }
        }

        String response = txid + " X " + responseChar;
        sendPacket(response, address, port);
        debugLog("Sent write response: " + response);
    }

    // **Handle a compare-and-swap request**
    private void handleCASRequest(String txid, String message, InetAddress address, int port) throws Exception {
        debugLog("Handling CAS request for TXID: " + txid + ", message: " + message);
        // Placeholder for CAS logic with debugging
        // Add full implementation with debug logs as needed
    }

    // **Handle a relay request**
    private void handleRelayRequest(String txid, String message, InetAddress address, int port) throws Exception {
        debugLog("Handling relay request for TXID: " + txid + ", message: " + message);
        // Placeholder for relay logic with debugging
        // Add full implementation with debug logs as needed
    }

    // **Calculate distance between two hash IDs**
    private int calculateDistance(byte[] hash1, byte[] hash2) {
        if (hash1 == null || hash2 == null || hash1.length != hash2.length) {
            debugLog("Invalid hash for distance calculation");
            return 256;
        }
        int matchingBits = 0;
        for (int i = 0; i < hash1.length; i++) {
            int xorByte = hash1[i] ^ hash2[i];
            if (xorByte == 0) {
                matchingBits += 8;
            } else {
                int mask = 0x80;
                while ((xorByte & mask) == 0 && mask > 0) {
                    matchingBits++;
                    mask >>= 1;
                }
                break;
            }
        }
        int distance = 256 - matchingBits;
        debugLog("Calculated distance: " + distance + " between hashes");
        return distance;
    }

    // **Find the nearest nodes to a target hash ID**
    private List<AddressKeyValuePair> findNearestNodes(byte[] targetHashID, int max) {
        debugLog("Finding nearest nodes to target hash ID");
        List<AddressKeyValuePair> allPairs = new ArrayList<>();
        for (List<AddressKeyValuePair> pairs : addressKeyValuesByDistance.values()) {
            for (AddressKeyValuePair pair : pairs) {
                try {
                    AddressKeyValuePair newPair = new AddressKeyValuePair(pair.key, pair.value);
                    newPair.distance = calculateDistance(targetHashID, pair.hashID);
                    allPairs.add(newPair);
                } catch (Exception e) {
                    errorLog("Failed to calculate distance for pair: " + pair.key);
                }
            }
        }
        Collections.sort(allPairs, Comparator.comparingInt(p -> p.distance));
        List<AddressKeyValuePair> nearest = allPairs.size() <= max ? allPairs : allPairs.subList(0, max);
        debugLog("Found " + nearest.size() + " nearest nodes");
        return nearest;
    }

    // **Determine if this node should store a key**
    private boolean shouldStoreKey(byte[] keyHashID) throws Exception {
        List<AddressKeyValuePair> nearestNodes = findNearestNodes(keyHashID, 3);
        int ourDistance = calculateDistance(nodeHashID, keyHashID);
        if (nearestNodes.size() < 3) {
            debugLog("Should store key: fewer than 3 nodes in network");
            return true;
        }
        for (AddressKeyValuePair pair : nearestNodes) {
            if (ourDistance < pair.distance) {
                debugLog("Should store key: closer than some nearest nodes");
                return true;
            }
        }
        debugLog("Should not store key: not among the closest nodes");
        return false;
    }

    // **Parse a CRN-formatted string**
    private String parseString(String message) {
        try {
            int spaceIndex = message.indexOf(' ');
            if (spaceIndex == -1) {
                debugLog("Failed to parse string: no space found in message");
                return null;
            }
            int count = Integer.parseInt(message.substring(0, spaceIndex));
            String content = message.substring(spaceIndex + 1);
            int spaceCount = 0;
            int endIndex = -1;
            for (int i = 0; i < content.length(); i++) {
                if (content.charAt(i) == ' ') {
                    spaceCount++;
                    if (spaceCount > count) {
                        endIndex = i;
                        break;
                    }
                }
            }
            if (endIndex == -1) {
                debugLog("Failed to parse string: space count mismatch");
                return null;
            }
            String parsed = content.substring(0, endIndex);
            debugLog("Parsed string: '" + parsed + "' from message: '" + message + "'");
            return parsed;
        } catch (Exception e) {
            errorLog("Exception while parsing string: " + e.getMessage());
            return null;
        }
    }

    // **Format a string in CRN format**
    private String formatString(String s) {
        int spaceCount = countSpaces(s);
        String formatted = spaceCount + " " + s + " ";
        debugLog("Formatted string: '" + formatted + "' for input: '" + s + "'");
        return formatted;
    }

    // **Format a key-value pair**
    private String formatKeyValuePair(String key, String value) {
        String formatted = formatString(key) + formatString(value);
        debugLog("Formatted key-value pair: '" + formatted + "'");
        return formatted;
    }

    // **Count spaces in a string**
    private int countSpaces(String s) {
        int count = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == ' ') count++;
        }
        return count;
    }

    // **Convert hex string to byte array**
    private byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    // **Convert byte array to hex string**
    private static String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }

    // **Generate a unique transaction ID**
    private String generateTransactionID() {
        byte[] txid = new byte[2];
        do {
            random.nextBytes(txid);
        } while (txid[0] == 0x20 || txid[1] == 0x20); // Avoid space characters
        String txidStr = new String(txid, StandardCharsets.ISO_8859_1);
        debugLog("Generated TXID: " + txidStr);
        return txidStr;
    }

    // **Send a UDP packet**
    private void sendPacket(String message, InetAddress address, int port) throws Exception {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
        socket.send(packet);
        debugLog("Sent packet to " + address + ":" + port + ": '" + message + "'");
    }

    // **Check if a node is active**
    @Override
    public boolean isActive(String nodeName) throws Exception {
        debugLog("Checking if node is active: " + nodeName);
        if (!keyValueStore.containsKey(nodeName)) {
            debugLog("Node not found in keyValueStore: " + nodeName);
            return false;
        }

        String nodeValue = keyValueStore.get(nodeName);
        String[] parts = nodeValue.split(":");
        if (parts.length != 2) {
            debugLog("Invalid node value format: " + nodeValue);
            return false;
        }

        InetAddress address = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);

        final boolean[] isActive = {false};
        final CountDownLatch latch = new CountDownLatch(1);

        String txid = generateTransactionID();
        String nameRequest = txid + " G";
        debugLog("Sending name request: " + nameRequest + " to " + address + ":" + port);

        ResponseCallback callback = new ResponseCallback() {
            @Override
            public void onResponse(String response) {
                debugLog("Received response for isActive: " + response);
                if (response.length() >= 5 && response.charAt(3) == 'H') {
                    isActive[0] = true;
                }
                latch.countDown();
            }

            @Override
            public void onTimeout() {
                debugLog("Timeout for isActive request to " + nodeName);
                latch.countDown();
            }
        };

        pendingTransactions.put(txid, callback);
        sendPacket(nameRequest, address, port);

        latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        debugLog("isActive result for " + nodeName + ": " + isActive[0]);
        return isActive[0];
    }

    // **Push a relay node onto the stack**
    @Override
    public void pushRelay(String nodeName) throws Exception {
        debugLog("Pushing relay node: " + nodeName);
        if (!keyValueStore.containsKey(nodeName)) {
            errorLog("Unknown relay node: " + nodeName);
            throw new Exception("Unknown relay node: " + nodeName);
        }
        relayStack.push(nodeName);
        debugLog("Relay stack after push: " + relayStack);
    }

    // **Pop a relay node from the stack**
    @Override
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            String popped = relayStack.pop();
            debugLog("Popped relay node: " + popped);
        } else {
            debugLog("Attempted to pop from empty relay stack");
        }
    }

    // **Check if a key exists in the DHT**
    @Override
    public boolean exists(String key) throws Exception {
        debugLog("Checking if key exists: " + key);
        if (keyValueStore.containsKey(key)) {
            debugLog("Key found locally: " + key);
            return true;
        }

        byte[] keyHash = HashID.computeHashID(key);
        List<String> closestNodes = findClosestNodeNames(keyHash);
        debugLog("Closest nodes for key " + key + ": " + closestNodes);

        for (String nodeName : closestNodes) {
            String nodeValue = keyValueStore.get(nodeName);
            if (nodeValue == null) continue;

            String[] parts = nodeValue.split(":");
            if (parts.length != 2) continue;

            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            final boolean[] exists = {false};
            final CountDownLatch latch = new CountDownLatch(1);

            String txid = generateTransactionID();
            String existsRequest = txid + " E " + formatString(key);
            debugLog("Sending exists request: " + existsRequest + " to " + nodeName);

            ResponseCallback callback = new ResponseCallback() {
                @Override
                public void onResponse(String response) {
                    debugLog("Received response for exists: " + response);
                    if (response.length() >= 5 && response.charAt(3) == 'F') {
                        exists[0] = response.charAt(5) == 'Y';
                    }
                    latch.countDown();
                }

                @Override
                public void onTimeout() {
                    debugLog("Timeout for exists request to " + nodeName);
                    latch.countDown();
                }
            };

            pendingTransactions.put(txid, callback);
            sendPacket(existsRequest, address, port);

            latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
            if (exists[0]) {
                debugLog("Key exists on node: " + nodeName);
                return true;
            }
        }
        debugLog("Key does not exist in DHT");
        return false;
    }

    // **Find the closest node names to a key hash**
    private List<String> findClosestNodeNames(byte[] keyHash) {
        debugLog("Finding closest node names for key hash");
        List<String> nodeNames = new ArrayList<>();
        List<AddressKeyValuePair> pairs = findNearestNodes(keyHash, 3);
        for (AddressKeyValuePair pair : pairs) {
            nodeNames.add(pair.key);
        }
        debugLog("Closest nodes: " + nodeNames);
        return nodeNames;
    }

    // **Read a value from the DHT**
    @Override
    public String read(String key) throws Exception {
        debugLog("Reading key: " + key);
        if (keyValueStore.containsKey(key)) {
            String value = keyValueStore.get(key);
            debugLog("Key found locally: " + key + " -> " + value);
            return value;
        }

        byte[] keyHash = HashID.computeHashID(key);
        List<String> closestNodes = findClosestNodeNames(keyHash);
        debugLog("Querying closest nodes for key: " + key + " - " + closestNodes);

        for (String nodeName : closestNodes) {
            String nodeValue = keyValueStore.get(nodeName);
            if (nodeValue == null) continue;

            String[] parts = nodeValue.split(":");
            if (parts.length != 2) continue;

            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
                final String[] value = {null};
                final CountDownLatch latch = new CountDownLatch(1);

                String txid = generateTransactionID();
                String readRequest = txid + " R " + formatString(key);
                debugLog("Sending read request: " + readRequest + " to " + nodeName);

                ResponseCallback callback = new ResponseCallback() {
                    @Override
                    public void onResponse(String response) {
                        debugLog("Received response for read: " + response);
                        if (response.length() >= 5 && response.charAt(3) == 'S') {
                            char result = response.charAt(5);
                            if (result == 'Y' && response.length() > 7) {
                                value[0] = parseString(response.substring(7));
                            }
                        }
                        latch.countDown();
                    }

                    @Override
                    public void onTimeout() {
                        debugLog("Timeout for read request to " + nodeName);
                        latch.countDown();
                    }
                };

                pendingTransactions.put(txid, callback);
                sendPacket(readRequest, address, port);

                if (latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS) && value[0] != null) {
                    keyValueStore.put(key, value[0]);
                    debugLog("Successfully read key: " + key + " -> " + value[0]);
                    return value[0];
                }
                debugLog("Retry " + (attempt + 1) + " for read request to " + nodeName);
            }
        }
        debugLog("Failed to read key: " + key);
        return null;
    }

    // **Write a value to the DHT**
    @Override
    public boolean write(String key, String value) throws Exception {
        debugLog("Writing key: " + key + ", value: " + value);
        if (key.startsWith("D:")) {
            keyValueStore.put(key, value);
            debugLog("Stored key locally: " + key + " -> " + value);
            return true;
        }

        byte[] keyHash = HashID.computeHashID(key);
        List<String> closestNodes = findClosestNodeNames(keyHash);
        debugLog("Writing to closest nodes: " + closestNodes);
        boolean success = false;

        for (String nodeName : closestNodes) {
            String nodeValue = keyValueStore.get(nodeName);
            if (nodeValue == null) continue;

            String[] parts = nodeValue.split(":");
            if (parts.length != 2) continue;

            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            final boolean[] writeSuccess = {false};
            final CountDownLatch latch = new CountDownLatch(1);

            String txid = generateTransactionID();
            String writeRequest = txid + " W " + formatKeyValuePair(key, value);
            debugLog("Sending write request: " + writeRequest + " to " + nodeName);

            ResponseCallback callback = new ResponseCallback() {
                @Override
                public void onResponse(String response) {
                    debugLog("Received response for write: " + response);
                    if (response.length() >= 5 && response.charAt(3) == 'X') {
                        char result = response.charAt(5);
                        writeSuccess[0] = (result == 'R' || result == 'A');
                    }
                    latch.countDown();
                }

                @Override
                public void onTimeout() {
                    debugLog("Timeout for write request to " + nodeName);
                    latch.countDown();
                }
            };

            pendingTransactions.put(txid, callback);
            sendPacket(writeRequest, address, port);

            latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
            if (writeSuccess[0]) {
                success = true;
                keyValueStore.put(key, value);
                debugLog("Write successful on node: " + nodeName);
            }
        }
        debugLog("Write operation " + (success ? "succeeded" : "failed") + " for key: " + key);
        return success || key.startsWith("D:");
    }

    // **Perform a compare-and-swap operation**
    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        debugLog("Performing CAS for key: " + key + ", currentValue: " + currentValue + ", newValue: " + newValue);
        if (keyValueStore.containsKey(key)) {
            String localValue = keyValueStore.get(key);
            if (localValue.equals(currentValue)) {
                keyValueStore.put(key, newValue);
                debugLog("CAS succeeded locally");
                return true;
            }
        }

        byte[] keyHash = HashID.computeHashID(key);
        List<String> closestNodes = findClosestNodeNames(keyHash);
        debugLog("Performing CAS on closest nodes: " + closestNodes);
        boolean success = false;

        for (String nodeName : closestNodes) {
            String nodeValue = keyValueStore.get(nodeName);
            if (nodeValue == null) continue;

            String[] parts = nodeValue.split(":");
            if (parts.length != 2) continue;

            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            final boolean[] casSuccess = {false};
            final CountDownLatch latch = new CountDownLatch(1);

            String txid = generateTransactionID();
            String casRequest = txid + " C " + formatString(key) + formatString(currentValue) + formatString(newValue);
            debugLog("Sending CAS request: " + casRequest + " to " + nodeName);

            ResponseCallback callback = new ResponseCallback() {
                @Override
                public void onResponse(String response) {
                    debugLog("Received response for CAS: " + response);
                    if (response.length() >= 5 && response.charAt(3) == 'D') {
                        char result = response.charAt(5);
                        casSuccess[0] = (result == 'R' || result == 'A');
                    }
                    latch.countDown();
                }

                @Override
                public void onTimeout() {
                    debugLog("Timeout for CAS request to " + nodeName);
                    latch.countDown();
                }
            };

            pendingTransactions.put(txid, callback);
            sendPacket(casRequest, address, port);

            latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
            if (casSuccess[0]) {
                success = true;
                keyValueStore.put(key, newValue);
                debugLog("CAS successful on node: " + nodeName);
                break;
            }
        }
        debugLog("CAS operation " + (success ? "succeeded" : "failed") + " for key: " + key);
        return success;
    }
}