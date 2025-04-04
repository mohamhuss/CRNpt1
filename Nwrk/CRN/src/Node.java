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

// **Node**: Implements the NodeInterface for a DHT node
public class Node implements NodeInterface {
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
    private AtomicInteger txidCounter = new AtomicInteger(0); // Ensures unique transaction IDs

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
            System.out.println("[DEBUG] Created AddressKeyValuePair for key: " + key + ", distance: " + distance);
        }
    }

    // Callback interface for handling asynchronous responses
    private interface ResponseCallback {
        void onResponse(String response);
        void onTimeout();
    }

    // Helper class for node addresses
    private class NodeAddress {
        String opisName;
        InetAddress address;
        int port;

        NodeAddress(String name, InetAddress address, int port) {
            this.opisName = name;
            this.address = address;
            this.port = port;
            System.out.println("[DEBUG] Created NodeAddress for node: " + name + " at " + address + ":" + port);
        }
    }

    // **Utility Class for HashID computation**
    private static class HashID {
        static byte[] computeHashID(String input) throws Exception {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            System.out.println("[DEBUG] Computed hash for input: " + input + ", hash: " + bytesToHex(hash));
            return hash;
        }
    }

    // **Set the node's name and compute its hash ID**
    @Override
    public void setNodeName(String nodeName) throws Exception {
        System.out.println("[DEBUG] Setting node name to: " + nodeName);
        if (!nodeName.startsWith("N:")) {
            System.err.println("[ERROR] Invalid node name: " + nodeName + " - must start with 'N:'");
            throw new IllegalArgumentException("Node name must start with N:");
        }
        this.nodeName = nodeName;
        this.nodeHashID = HashID.computeHashID(nodeName);
        System.out.println("[DEBUG] Node hash ID computed: " + bytesToHex(this.nodeHashID));
    }

    // **Open a UDP port for communication**
    @Override
    public void openPort(int portNumber) throws Exception {
        System.out.println("[DEBUG] Opening port: " + portNumber);
        this.port = portNumber;

        try {
            this.socket = new DatagramSocket(portNumber);
            System.out.println("[DEBUG] Successfully bound to port: " + portNumber);
        } catch (BindException e) {
            System.out.println("[DEBUG] BindException on port " + portNumber + ": " + e.getMessage());
            boolean success = false;
            for (int offset = 1; offset <= 5; offset++) {
                try {
                    this.socket = new DatagramSocket(portNumber + offset);
                    this.port = portNumber + offset;
                    System.out.println("[DEBUG] Successfully bound to alternative port: " + this.port);
                    success = true;
                    break;
                } catch (BindException be) {
                    System.out.println("[DEBUG] Failed to bind to port " + (portNumber + offset));
                }
            }
            if (!success) {
                System.err.println("[ERROR] Could not find an available port. Tried ports " + portNumber + " through " + (portNumber + 5));
                throw new Exception("Could not find an available port.");
            }
        }

        String localIP = InetAddress.getLocalHost().getHostAddress();
        String value = localIP + ":" + this.port;
        keyValueStore.put(nodeName, value);
        System.out.println("[DEBUG] Stored self address: " + nodeName + " -> " + value);

        AddressKeyValuePair selfPair = new AddressKeyValuePair(nodeName, value);
        addAddressKeyValuePair(selfPair);

        startListenerThread();
    }

    // **Start a background thread to listen for incoming messages**
    private void startListenerThread() {
        System.out.println("[DEBUG] Starting listener thread");
        listenerThread = new Thread(() -> {
            try {
                while (!socket.isClosed()) {
                    byte[] buffer = new byte[MAX_PACKET_SIZE];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    System.out.println("[DEBUG] Waiting for incoming packet...");
                    socket.receive(packet);
                    String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                    System.out.println("[DEBUG] Received message: '" + message + "' from " + packet.getAddress() + ":" + packet.getPort());
                    executor.submit(() -> {
                        try {
                            handleBootstrapMessage(message, packet.getAddress(), packet.getPort());
                            processMessage(packet);
                        } catch (Exception e) {
                            System.err.println("[ERROR] Error handling message: " + e.getMessage());
                        }
                    });
                }
            } catch (SocketTimeoutException e) {
                System.out.println("[DEBUG] Socket timeout in listener: " + e.getMessage());
            } catch (IOException e) {
                if (!socket.isClosed()) {
                    System.err.println("[ERROR] Socket error in listener: " + e.getMessage());
                }
            }
        });
        listenerThread.setDaemon(true);
        listenerThread.start();
        System.out.println("[DEBUG] Listener thread started");
    }

    // **Handle bootstrap messages to learn about other nodes**
    private void handleBootstrapMessage(String message, InetAddress sourceAddress, int sourcePort) {
        System.out.println("[DEBUG] Handling bootstrap message: " + message);
        if (message.contains("W") && message.contains("N:")) {
            String[] parts = message.split(" ");
            if (parts.length >= 6 && parts[1].equals("W")) {
                try {
                    int keySpaceCount = Integer.parseInt(parts[2]);
                    String key = parts[3];
                    if (!key.startsWith("N:")) {
                        System.out.println("[DEBUG] Invalid node name in bootstrap: " + key);
                        return;
                    }

                    int valueSpaceCount = Integer.parseInt(parts[4]);
                    String value = parts[5];
                    if (!value.contains(":")) {
                        System.out.println("[DEBUG] Invalid address format in bootstrap: " + value);
                        return;
                    }
                    String[] addrParts = value.split(":");
                    if (addrParts.length != 2) {
                        System.out.println("[DEBUG] Invalid address parts in bootstrap: " + value);
                        return;
                    }
                    String ip = addrParts[0];
                    int port = Integer.parseInt(addrParts[1]);

                    InetAddress addr = InetAddress.getByName(ip);
                    NodeAddress nodeAddr = new NodeAddress(key, addr, port);
                    addNodeAddress(key, nodeAddr);
                    keyValueStore.put(key, value);
                    System.out.println("[DEBUG] Added node from bootstrap: " + key + " -> " + value);
                } catch (Exception e) {
                    System.err.println("[ERROR] Failed to parse bootstrap message: " + message + " - " + e.getMessage());
                }
            } else {
                System.out.println("[DEBUG] Bootstrap message format invalid: " + message);
            }
        } else {
            System.out.println("[DEBUG] Ignored non-bootstrap message: " + message);
        }
    }

    // **Add a node address to the routing table**
    private void addNodeAddress(String nodeName, NodeAddress nodeAddr) {
        System.out.println("[DEBUG] Adding node address for: " + nodeName);
        try {
            AddressKeyValuePair pair = new AddressKeyValuePair(nodeName,
                    nodeAddr.address.getHostAddress() + ":" + nodeAddr.port);
            addAddressKeyValuePair(pair);
            System.out.println("[DEBUG] Successfully added node address: " + nodeName);
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to add node address: " + e.getMessage());
        }
    }

    // **Add an address key-value pair to the routing table**
    private void addAddressKeyValuePair(AddressKeyValuePair pair) {
        keyValueStore.put(pair.key, pair.value);
        System.out.println("[DEBUG] Stored key-value pair: " + pair.key + " -> " + pair.value);

        List<AddressKeyValuePair> pairsAtDistance = addressKeyValuesByDistance.computeIfAbsent(
                pair.distance, k -> new CopyOnWriteArrayList<>());

        boolean exists = false;
        for (int i = 0; i < pairsAtDistance.size(); i++) {
            if (pairsAtDistance.get(i).key.equals(pair.key)) {
                pairsAtDistance.set(i, pair);
                exists = true;
                System.out.println("[DEBUG] Updated existing node in routing table: " + pair.key);
                break;
            }
        }

        if (!exists) {
            pairsAtDistance.add(pair);
            System.out.println("[DEBUG] Added new node to routing table: " + pair.key + " at distance " + pair.distance);
            if (pairsAtDistance.size() > 3) {
                pairsAtDistance.remove(pairsAtDistance.size() - 1);
                System.out.println("[DEBUG] Removed least recent node from distance " + pair.distance);
            }
        }
    }

    // **Handle incoming messages with a specified delay**
    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        System.out.println("[DEBUG] Handling incoming messages with delay: " + delay + " ms");
        if (socket == null) {
            System.err.println("[ERROR] Socket not initialized. Call openPort first.");
            throw new IllegalStateException("Socket not initialized.");
        }

        if (delay <= 0) {
            System.out.println("[DEBUG] Delay <= 0, sleeping for 10 seconds");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("[DEBUG] Interrupted during sleep");
            }
            return;
        }

        try {
            Thread.sleep(delay);
            System.out.println("[DEBUG] Slept for " + delay + " ms");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("[DEBUG] Interrupted during sleep");
        }
    }

    // **Process an incoming UDP message**
    private void processMessage(DatagramPacket packet) {
        String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
        System.out.println("[DEBUG] Processing message: " + message);
        if (message.length() < 4) {
            System.out.println("[DEBUG] Message too short: " + message);
            return;
        }

        try {
            String txid = message.substring(0, 2);
            char messageType = message.charAt(3);
            System.out.println("[DEBUG] Parsed TXID: " + txid + ", Message Type: " + messageType);

            ResponseCallback callback = pendingTransactions.remove(txid);
            if (callback != null) {
                System.out.println("[DEBUG] Found pending transaction for TXID: " + txid);
                callback.onResponse(message);
                return;
            }

            switch (messageType) {
                case 'G': // Name request
                    System.out.println("[DEBUG] Handling name request");
                    handleNameRequest(txid, packet.getAddress(), packet.getPort());
                    break;
                case 'N': // Nearest request
                    if (message.length() > 5) {
                        System.out.println("[DEBUG] Handling nearest request");
                        handleNearestRequest(txid, message.substring(5), packet.getAddress(), packet.getPort());
                    }
                    break;
                case 'E': // Key existence request
                    if (message.length() > 5) {
                        System.out.println("[DEBUG] Handling key existence request");
                        handleKeyExistenceRequest(txid, message.substring(5), packet.getAddress(), packet.getPort());
                    }
                    break;
                case 'R': // Read request
                    if (message.length() > 5) {
                        System.out.println("[DEBUG] Handling read request");
                        handleReadRequest(txid, message.substring(5), packet.getAddress(), packet.getPort());
                    }
                    break;
                case 'W': // Write request
                    if (message.length() > 5) {
                        System.out.println("[DEBUG] Handling write request");
                        handleWriteRequest(txid, message.substring(5), packet.getAddress(), packet.getPort());
                    }
                    break;
                case 'C': // Compare and swap request
                    if (message.length() > 5) {
                        System.out.println("[DEBUG] Handling CAS request");
                        handleCASRequest(txid, message.substring(5), packet.getAddress(), packet.getPort());
                    }
                    break;
                case 'V': // Relay request
                    if (message.length() > 5) {
                        System.out.println("[DEBUG] Handling relay request");
                        handleRelayRequest(txid, message.substring(5), packet.getAddress(), packet.getPort());
                    }
                    break;
                case 'I': // Information message
                    System.out.println("[DEBUG] Received information message: " + message);
                    break;
                default:
                    System.out.println("[DEBUG] Unknown message type: " + messageType);
            }
        } catch (Exception e) {
            System.err.println("[ERROR] Exception in processMessage: " + e.getMessage());
        }
    }

    // **Handle a name request**
    private void handleNameRequest(String txid, InetAddress address, int port) throws Exception {
        System.out.println("[DEBUG] Handling name request for TXID: " + txid);
        String response = txid + " H " + formatString(nodeName);
        sendPacket(response, address, port);
        System.out.println("[DEBUG] Sent name response: " + response);
    }

    // **Handle a nearest node request**
    private void handleNearestRequest(String txid, String hashIDHex, InetAddress address, int port) throws Exception {
        System.out.println("[DEBUG] Handling nearest request for TXID: " + txid + ", hashIDHex: " + hashIDHex);
        try {
            byte[] targetHashID = hexStringToByteArray(hashIDHex.trim());
            List<AddressKeyValuePair> nearestNodes = findNearestNodes(targetHashID, 3);

            StringBuilder response = new StringBuilder(txid + " O ");
            for (AddressKeyValuePair pair : nearestNodes) {
                response.append(formatKeyValuePair(pair.key, pair.value));
            }
            sendPacket(response.toString(), address, port);
            System.out.println("[DEBUG] Sent nearest nodes response: " + response.toString());
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to handle nearest request: " + e.getMessage());
        }
    }

    // **Handle a key existence request**
    private void handleKeyExistenceRequest(String txid, String keyString, InetAddress address, int port) throws Exception {
        System.out.println("[DEBUG] Handling key existence request for TXID: " + txid + ", keyString: " + keyString);
        String key = parseString(keyString);
        if (key == null) {
            System.out.println("[DEBUG] Failed to parse key from keyString: " + keyString);
            return;
        }
        char responseChar;
        if (keyValueStore.containsKey(key)) {
            responseChar = 'Y';
            System.out.println("[DEBUG] Key exists locally: " + key);
        } else {
            byte[] keyHashID = HashID.computeHashID(key);
            responseChar = shouldStoreKey(keyHashID) ? 'N' : '?';
            System.out.println("[DEBUG] Key does not exist locally, response: " + responseChar);
        }
        String response = txid + " F " + responseChar;
        sendPacket(response, address, port);
        System.out.println("[DEBUG] Sent key existence response: " + response);
    }

    // **Handle a read request**
    private void handleReadRequest(String txid, String keyString, InetAddress address, int port) throws Exception {
        System.out.println("[DEBUG] Handling read request for TXID: " + txid + ", keyString: " + keyString);
        String key = parseString(keyString);
        if (key == null) {
            System.out.println("[DEBUG] Failed to parse key from keyString: " + keyString);
            return;
        }
        char responseChar;
        String value = "";
        if (keyValueStore.containsKey(key)) {
            responseChar = 'Y';
            value = keyValueStore.get(key);
            System.out.println("[DEBUG] Read key locally: " + key + " -> " + value);
        } else {
            byte[] keyHashID = HashID.computeHashID(key);
            responseChar = shouldStoreKey(keyHashID) ? 'N' : '?';
            System.out.println("[DEBUG] Key not found locally, response: " + responseChar);
        }
        String response = txid + " S " + responseChar + " " + formatString(value);
        sendPacket(response, address, port);
        System.out.println("[DEBUG] Sent read response: " + response);
    }

    // **Handle a write request**
    private void handleWriteRequest(String txid, String message, InetAddress address, int port) throws Exception {
        System.out.println("[DEBUG] Handling write request for TXID: " + txid + ", message: " + message);
        String[] parts = message.split(" ", 3);
        if (parts.length < 3) {
            System.out.println("[DEBUG] Invalid write request format: " + message);
            return;
        }
        int keySpaceCount = Integer.parseInt(parts[0]);
        String key = parts[1];
        if (countSpaces(key) != keySpaceCount) {
            System.out.println("[DEBUG] Space count mismatch for key: " + key);
            return;
        }
        String[] valueParts = parts[2].split(" ", 2);
        if (valueParts.length < 2) {
            System.out.println("[DEBUG] Invalid value format in write request: " + parts[2]);
            return;
        }
        int valueSpaceCount = Integer.parseInt(valueParts[0]);
        String value = valueParts[1];
        if (countSpaces(value) != valueSpaceCount) {
            System.out.println("[DEBUG] Space count mismatch for value: " + value);
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
                    System.out.println("[DEBUG] Added node from write request: " + key + " -> " + value);
                }
            } catch (Exception e) {
                System.err.println("[ERROR] Failed to add node from write request: " + e.getMessage());
            }
        }

        if (keyValueStore.containsKey(key)) {
            keyValueStore.put(key, value);
            responseChar = 'R';
            System.out.println("[DEBUG] Updated existing key: " + key + " -> " + value);
        } else {
            if (key.startsWith("D:") || address.isLoopbackAddress()) {
                keyValueStore.put(key, value);
                responseChar = 'A';
                System.out.println("[DEBUG] Added new key (D: or loopback): " + key + " -> " + value);
            } else {
                byte[] keyHashID = HashID.computeHashID(key);
                responseChar = shouldStoreKey(keyHashID) ? 'A' : 'X';
                if (responseChar == 'A') {
                    keyValueStore.put(key, value);
                    System.out.println("[DEBUG] Accepted new key: " + key + " -> " + value);
                } else {
                    System.out.println("[DEBUG] Rejected write for key: " + key + " (not in storage range)");
                }
            }
        }

        String response = txid + " X " + responseChar;
        sendPacket(response, address, port);
        System.out.println("[DEBUG] Sent write response: " + response);
    }

    // **Handle a compare-and-swap request**
    private void handleCASRequest(String txid, String message, InetAddress address, int port) throws Exception {
        System.out.println("[DEBUG] Handling CAS request for TXID: " + txid + ", message: " + message);
        int firstSpace = message.indexOf(' ');
        if (firstSpace == -1) {
            System.out.println("[DEBUG] Invalid CAS request format: " + message);
            return;
        }
        String keyCountStr = message.substring(0, firstSpace);
        int keyCount = Integer.parseInt(keyCountStr);
        String rest = message.substring(firstSpace + 1);
        int keyEnd = -1;
        int spaceCount = 0;
        for (int i = 0; i < rest.length(); i++) {
            if (rest.charAt(i) == ' ') {
                spaceCount++;
                if (spaceCount > keyCount) {
                    keyEnd = i;
                    break;
                }
            }
        }
        if (keyEnd == -1) {
            System.out.println("[DEBUG] Failed to parse key from CAS request");
            return;
        }
        String key = rest.substring(0, keyEnd);
        String expectedAndNew = rest.substring(keyEnd + 1);
        firstSpace = expectedAndNew.indexOf(' ');
        if (firstSpace == -1) {
            System.out.println("[DEBUG] Invalid expected value format in CAS request");
            return;
        }
        String expectedCountStr = expectedAndNew.substring(0, firstSpace);
        int expectedCount = Integer.parseInt(expectedCountStr);
        rest = expectedAndNew.substring(firstSpace + 1);
        int expectedEnd = -1;
        spaceCount = 0;
        for (int i = 0; i < rest.length(); i++) {
            if (rest.charAt(i) == ' ') {
                spaceCount++;
                if (spaceCount > expectedCount) {
                    expectedEnd = i;
                    break;
                }
            }
        }
        if (expectedEnd == -1) {
            System.out.println("[DEBUG] Failed to parse expected value from CAS request");
            return;
        }
        String expectedValue = rest.substring(0, expectedEnd);
        String newValuePart = rest.substring(expectedEnd + 1);
        firstSpace = newValuePart.indexOf(' ');
        if (firstSpace == -1) {
            System.out.println("[DEBUG] Invalid new value format in CAS request");
            return;
        }
        String newCountStr = newValuePart.substring(0, firstSpace);
        String newValue = newValuePart.substring(firstSpace + 1);
        char responseChar;
        if (keyValueStore.containsKey(key)) {
            String currentValue = keyValueStore.get(key);
            if (currentValue.equals(expectedValue)) {
                keyValueStore.put(key, newValue);
                responseChar = 'R';
                System.out.println("[DEBUG] CAS updated key: " + key + " to " + newValue);
            } else {
                responseChar = 'N';
                System.out.println("[DEBUG] CAS failed: expected value mismatch for key " + key);
            }
        } else {
            byte[] keyHashID = HashID.computeHashID(key);
            if (shouldStoreKey(keyHashID)) {
                keyValueStore.put(key, newValue);
                responseChar = 'A';
                System.out.println("[DEBUG] CAS added new key: " + key + " -> " + newValue);
            } else {
                responseChar = 'X';
                System.out.println("[DEBUG] CAS rejected for key: " + key + " (not in storage range)");
            }
        }
        String response = txid + " D " + responseChar;
        sendPacket(response, address, port);
        System.out.println("[DEBUG] Sent CAS response: " + response);
    }

    // **Handle a relay request**
    private void handleRelayRequest(String txid, String message, InetAddress address, int port) throws Exception {
        System.out.println("[DEBUG] Handling relay request for TXID: " + txid + ", message: " + message);
        String destNodeName = parseString(message);
        if (destNodeName == null) {
            System.out.println("[DEBUG] Failed to parse destination node from relay request");
            return;
        }
        int destNameEndPos = message.indexOf(' ', message.indexOf(' ') + 1) + 1;
        String innerMessage = message.substring(destNameEndPos);
        String innerTxid = innerMessage.substring(0, 2);
        String nodeValue = keyValueStore.get(destNodeName);
        if (nodeValue == null) {
            System.out.println("[DEBUG] Destination node not found in keyValueStore: " + destNodeName);
            return;
        }
        String[] parts = nodeValue.split(":");
        if (parts.length != 2) {
            System.out.println("[DEBUG] Invalid node value format: " + nodeValue);
            return;
        }
        InetAddress destAddress = InetAddress.getByName(parts[0]);
        int destPort = Integer.parseInt(parts[1]);
        ResponseCallback callback = new ResponseCallback() {
            @Override
            public void onResponse(String response) {
                try {
                    String relayResponse = txid + response.substring(2);
                    sendPacket(relayResponse, address, port);
                    System.out.println("[DEBUG] Sent relay response: " + relayResponse);
                } catch (Exception e) {
                    System.err.println("[ERROR] Failed to send relay response: " + e.getMessage());
                }
            }

            @Override
            public void onTimeout() {
                System.out.println("[DEBUG] Timeout for relay request TXID: " + innerTxid);
            }
        };
        pendingTransactions.put(innerTxid, callback);
        sendPacket(innerMessage, destAddress, destPort);
        System.out.println("[DEBUG] Sent inner message for relay: " + innerMessage + " to " + destAddress + ":" + destPort);
    }

    // **Calculate distance between two hash IDs**
    private int calculateDistance(byte[] hash1, byte[] hash2) {
        if (hash1 == null || hash2 == null || hash1.length != hash2.length) {
            System.out.println("[DEBUG] Invalid hash for distance calculation");
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
        System.out.println("[DEBUG] Calculated distance: " + distance);
        return distance;
    }

    // **Find the nearest nodes to a target hash ID**
    private List<AddressKeyValuePair> findNearestNodes(byte[] targetHashID, int max) {
        System.out.println("[DEBUG] Finding nearest nodes to target hash ID");
        List<AddressKeyValuePair> allPairs = new ArrayList<>();
        for (List<AddressKeyValuePair> pairs : addressKeyValuesByDistance.values()) {
            for (AddressKeyValuePair pair : pairs) {
                try {
                    AddressKeyValuePair newPair = new AddressKeyValuePair(pair.key, pair.value);
                    newPair.distance = calculateDistance(targetHashID, pair.hashID);
                    allPairs.add(newPair);
                } catch (Exception e) {
                    System.err.println("[ERROR] Failed to calculate distance for pair: " + pair.key + " - " + e.getMessage());
                }
            }
        }
        Collections.sort(allPairs, Comparator.comparingInt(p -> p.distance));
        List<AddressKeyValuePair> nearest = allPairs.size() <= max ? allPairs : allPairs.subList(0, max);
        System.out.println("[DEBUG] Found " + nearest.size() + " nearest nodes");
        return nearest;
    }

    // **Determine if this node should store a key**
    private boolean shouldStoreKey(byte[] keyHashID) throws Exception {
        System.out.println("[DEBUG] Determining if node should store key with hash: " + bytesToHex(keyHashID));
        List<AddressKeyValuePair> nearestNodes = findNearestNodes(keyHashID, 3);
        int ourDistance = calculateDistance(nodeHashID, keyHashID);
        if (nearestNodes.size() < 3) {
            System.out.println("[DEBUG] Should store key: fewer than 3 nodes in network");
            return true;
        }
        for (AddressKeyValuePair pair : nearestNodes) {
            if (ourDistance < pair.distance) {
                System.out.println("[DEBUG] Should store key: closer than some nearest nodes");
                return true;
            }
        }
        System.out.println("[DEBUG] Should not store key: not among the closest nodes");
        return false;
    }

    // **Parse a CRN-formatted string**
    private String parseString(String message) {
        System.out.println("[DEBUG] Parsing string: " + message);
        try {
            int spaceIndex = message.indexOf(' ');
            if (spaceIndex == -1) {
                System.out.println("[DEBUG] Failed to parse: no space found");
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
                System.out.println("[DEBUG] Failed to parse: space count mismatch");
                return null;
            }
            String parsed = content.substring(0, endIndex);
            System.out.println("[DEBUG] Parsed string: " + parsed);
            return parsed;
        } catch (Exception e) {
            System.err.println("[ERROR] Exception while parsing string: " + e.getMessage());
            return null;
        }
    }

    // **Format a string in CRN format**
    private String formatString(String s) {
        int spaceCount = countSpaces(s);
        String formatted = spaceCount + " " + s + " ";
        System.out.println("[DEBUG] Formatted string: '" + formatted + "' from: '" + s + "'");
        return formatted;
    }

    // **Format a key-value pair**
    private String formatKeyValuePair(String key, String value) {
        String formatted = formatString(key) + formatString(value);
        System.out.println("[DEBUG] Formatted key-value pair: " + formatted);
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
        System.out.println("[DEBUG] Converting hex string to byte array: " + s);
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
        System.out.println("[DEBUG] Generated TXID: " + txidStr);
        return txidStr;
    }

    // **Send a UDP packet**
    private void sendPacket(String message, InetAddress address, int port) throws Exception {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
        socket.send(packet);
        System.out.println("[DEBUG] Sent packet to " + address + ":" + port + ": '" + message + "'");
    }

    // **Check if a node is active**
    @Override
    public boolean isActive(String nodeName) throws Exception {
        System.out.println("[DEBUG] Checking if node is active: " + nodeName);
        if (!keyValueStore.containsKey(nodeName)) {
            System.out.println("[DEBUG] Node not found in keyValueStore: " + nodeName);
            return false;
        }

        String nodeValue = keyValueStore.get(nodeName);
        String[] parts = nodeValue.split(":");
        if (parts.length != 2) {
            System.out.println("[DEBUG] Invalid node value format: " + nodeValue);
            return false;
        }

        InetAddress address = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);

        final boolean[] isActive = {false};
        final CountDownLatch latch = new CountDownLatch(1);

        String txid = generateTransactionID();
        String nameRequest = txid + " G";
        System.out.println("[DEBUG] Sending name request: " + nameRequest + " to " + address + ":" + port);

        ResponseCallback callback = new ResponseCallback() {
            @Override
            public void onResponse(String response) {
                System.out.println("[DEBUG] Received response for isActive: " + response);
                if (response.length() >= 5 && response.charAt(3) == 'H') {
                    isActive[0] = true;
                }
                latch.countDown();
            }

            @Override
            public void onTimeout() {
                System.out.println("[DEBUG] Timeout for isActive request to " + nodeName);
                latch.countDown();
            }
        };

        pendingTransactions.put(txid, callback);
        sendPacket(nameRequest, address, port);

        latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        System.out.println("[DEBUG] isActive result for " + nodeName + ": " + isActive[0]);
        return isActive[0];
    }

    // **Push a relay node onto the stack**
    @Override
    public void pushRelay(String nodeName) throws Exception {
        System.out.println("[DEBUG] Pushing relay node: " + nodeName);
        if (!keyValueStore.containsKey(nodeName)) {
            System.err.println("[ERROR] Unknown relay node: " + nodeName);
            throw new Exception("Unknown relay node: " + nodeName);
        }
        relayStack.push(nodeName);
        System.out.println("[DEBUG] Relay stack after push: " + relayStack);
    }

    // **Pop a relay node from the stack**
    @Override
    public void popRelay() throws Exception {
        System.out.println("[DEBUG] Popping relay node");
        if (!relayStack.isEmpty()) {
            String popped = relayStack.pop();
            System.out.println("[DEBUG] Popped relay node: " + popped);
        } else {
            System.out.println("[DEBUG] Relay stack is empty");
        }
    }

    // **Check if a key exists in the DHT**
    @Override
    public boolean exists(String key) throws Exception {
        System.out.println("[DEBUG] Checking if key exists: " + key);
        if (keyValueStore.containsKey(key)) {
            System.out.println("[DEBUG] Key found locally: " + key);
            return true;
        }

        byte[] keyHash = HashID.computeHashID(key);
        List<String> closestNodes = findClosestNodeNames(keyHash);
        System.out.println("[DEBUG] Closest nodes for key " + key + ": " + closestNodes);

        for (String nodeName : closestNodes) {
            String nodeValue = keyValueStore.get(nodeName);
            if (nodeValue == null) {
                System.out.println("[DEBUG] No value for node: " + nodeName);
                continue;
            }

            String[] parts = nodeValue.split(":");
            if (parts.length != 2) {
                System.out.println("[DEBUG] Invalid node value format: " + nodeValue);
                continue;
            }

            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            final boolean[] exists = {false};
            final CountDownLatch latch = new CountDownLatch(1);

            String txid = generateTransactionID();
            String existsRequest = txid + " E " + formatString(key);
            System.out.println("[DEBUG] Sending exists request: " + existsRequest + " to " + nodeName);

            ResponseCallback callback = new ResponseCallback() {
                @Override
                public void onResponse(String response) {
                    System.out.println("[DEBUG] Received response for exists: " + response);
                    if (response.length() >= 5 && response.charAt(3) == 'F') {
                        exists[0] = response.charAt(5) == 'Y';
                    }
                    latch.countDown();
                }

                @Override
                public void onTimeout() {
                    System.out.println("[DEBUG] Timeout for exists request to " + nodeName);
                    latch.countDown();
                }
            };

            pendingTransactions.put(txid, callback);
            sendPacket(existsRequest, address, port);

            latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
            if (exists[0]) {
                System.out.println("[DEBUG] Key exists on node: " + nodeName);
                return true;
            }
        }
        System.out.println("[DEBUG] Key does not exist in DHT");
        return false;
    }

    // **Find the closest node names to a key hash**
    private List<String> findClosestNodeNames(byte[] keyHash) {
        System.out.println("[DEBUG] Finding closest node names for key hash");
        List<String> nodeNames = new ArrayList<>();
        List<AddressKeyValuePair> pairs = findNearestNodes(keyHash, 3);
        for (AddressKeyValuePair pair : pairs) {
            nodeNames.add(pair.key);
        }
        System.out.println("[DEBUG] Closest nodes: " + nodeNames);
        return nodeNames;
    }

    // **Read a value from the DHT**
    @Override
    public String read(String key) throws Exception {
        System.out.println("[DEBUG] Reading key: " + key);
        if (keyValueStore.containsKey(key)) {
            String value = keyValueStore.get(key);
            System.out.println("[DEBUG] Key found locally: " + key + " -> " + value);
            return value;
        }

        byte[] keyHash = HashID.computeHashID(key);
        List<String> closestNodes = findClosestNodeNames(keyHash);
        System.out.println("[DEBUG] Querying closest nodes for key: " + key + " - " + closestNodes);

        for (String nodeName : closestNodes) {
            String nodeValue = keyValueStore.get(nodeName);
            if (nodeValue == null) {
                System.out.println("[DEBUG] No value for node: " + nodeName);
                continue;
            }

            String[] parts = nodeValue.split(":");
            if (parts.length != 2) {
                System.out.println("[DEBUG] Invalid node value format: " + nodeValue);
                continue;
            }

            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
                final String[] value = {null};
                final CountDownLatch latch = new CountDownLatch(1);

                String txid = generateTransactionID();
                String readRequest = txid + " R " + formatString(key);
                System.out.println("[DEBUG] Sending read request: " + readRequest + " to " + nodeName + " (attempt " + (attempt + 1) + ")");

                ResponseCallback callback = new ResponseCallback() {
                    @Override
                    public void onResponse(String response) {
                        System.out.println("[DEBUG] Received response for read: " + response);
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
                        System.out.println("[DEBUG] Timeout for read request to " + nodeName);
                        latch.countDown();
                    }
                };

                pendingTransactions.put(txid, callback);
                sendPacket(readRequest, address, port);

                if (latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS) && value[0] != null) {
                    keyValueStore.put(key, value[0]);
                    System.out.println("[DEBUG] Successfully read key: " + key + " -> " + value[0]);
                    return value[0];
                }
                System.out.println("[DEBUG] Retry " + (attempt + 1) + " failed for read request to " + nodeName);
            }
        }
        System.out.println("[DEBUG] Failed to read key: " + key + " after all retries");
        return null;
    }

    // **Write a value to the DHT**
    @Override
    public boolean write(String key, String value) throws Exception {
        System.out.println("[DEBUG] Writing key: " + key + ", value: " + value);
        if (key.startsWith("D:")) {
            keyValueStore.put(key, value);
            System.out.println("[DEBUG] Stored key locally (D: prefix): " + key + " -> " + value);
            return true;
        }

        byte[] keyHash = HashID.computeHashID(key);
        List<String> closestNodes = findClosestNodeNames(keyHash);
        System.out.println("[DEBUG] Writing to closest nodes: " + closestNodes);
        boolean success = false;

        for (String nodeName : closestNodes) {
            String nodeValue = keyValueStore.get(nodeName);
            if (nodeValue == null) {
                System.out.println("[DEBUG] No value for node: " + nodeName);
                continue;
            }

            String[] parts = nodeValue.split(":");
            if (parts.length != 2) {
                System.out.println("[DEBUG] Invalid node value format: " + nodeValue);
                continue;
            }

            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            final boolean[] writeSuccess = {false};
            final CountDownLatch latch = new CountDownLatch(1);

            String txid = generateTransactionID();
            String writeRequest = txid + " W " + formatKeyValuePair(key, value);
            System.out.println("[DEBUG] Sending write request: " + writeRequest + " to " + nodeName);

            ResponseCallback callback = new ResponseCallback() {
                @Override
                public void onResponse(String response) {
                    System.out.println("[DEBUG] Received response for write: " + response);
                    if (response.length() >= 5 && response.charAt(3) == 'X') {
                        char result = response.charAt(5);
                        writeSuccess[0] = (result == 'R' || result == 'A');
                    }
                    latch.countDown();
                }

                @Override
                public void onTimeout() {
                    System.out.println("[DEBUG] Timeout for write request to " + nodeName);
                    latch.countDown();
                }
            };

            pendingTransactions.put(txid, callback);
            sendPacket(writeRequest, address, port);

            latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
            if (writeSuccess[0]) {
                success = true;
                keyValueStore.put(key, value);
                System.out.println("[DEBUG] Write successful on node: " + nodeName);
            }
        }
        System.out.println("[DEBUG] Write operation " + (success ? "succeeded" : "failed") + " for key: " + key);
        return success || key.startsWith("D:");
    }

    // **Perform a compare-and-swap operation**
    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        System.out.println("[DEBUG] Performing CAS for key: " + key + ", currentValue: " + currentValue + ", newValue: " + newValue);
        if (keyValueStore.containsKey(key)) {
            String localValue = keyValueStore.get(key);
            if (localValue.equals(currentValue)) {
                keyValueStore.put(key, newValue);
                System.out.println("[DEBUG] CAS succeeded locally: " + key + " updated to " + newValue);
                return true;
            } else {
                System.out.println("[DEBUG] CAS failed locally: value mismatch for " + key);
            }
        }

        byte[] keyHash = HashID.computeHashID(key);
        List<String> closestNodes = findClosestNodeNames(keyHash);
        System.out.println("[DEBUG] Performing CAS on closest nodes: " + closestNodes);
        boolean success = false;

        for (String nodeName : closestNodes) {
            String nodeValue = keyValueStore.get(nodeName);
            if (nodeValue == null) {
                System.out.println("[DEBUG] No value for node: " + nodeName);
                continue;
            }

            String[] parts = nodeValue.split(":");
            if (parts.length != 2) {
                System.out.println("[DEBUG] Invalid node value format: " + nodeValue);
                continue;
            }

            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            final boolean[] casSuccess = {false};
            final CountDownLatch latch = new CountDownLatch(1);

            String txid = generateTransactionID();
            String casRequest = txid + " C " + formatString(key) + formatString(currentValue) + formatString(newValue);
            System.out.println("[DEBUG] Sending CAS request: " + casRequest + " to " + nodeName);

            ResponseCallback callback = new ResponseCallback() {
                @Override
                public void onResponse(String response) {
                    System.out.println("[DEBUG] Received response for CAS: " + response);
                    if (response.length() >= 5 && response.charAt(3) == 'D') {
                        char result = response.charAt(5);
                        casSuccess[0] = (result == 'R' || result == 'A');
                    }
                    latch.countDown();
                }

                @Override
                public void onTimeout() {
                    System.out.println("[DEBUG] Timeout for CAS request to " + nodeName);
                    latch.countDown();
                }
            };

            pendingTransactions.put(txid, callback);
            sendPacket(casRequest, address, port);

            latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
            if (casSuccess[0]) {
                success = true;
                keyValueStore.put(key, newValue);
                System.out.println("[DEBUG] CAS successful on node: " + nodeName);
                break;
            }
        }
        System.out.println("[DEBUG] CAS operation " + (success ? "succeeded" : "failed") + " for key: " + key);
        return success;
    }
}