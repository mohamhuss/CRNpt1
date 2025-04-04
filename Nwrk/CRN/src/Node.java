import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

// DO NOT EDIT starts
interface NodeInterface {
    public void setNodeName(String nodeName) throws Exception;
    public void openPort(int portNumber) throws Exception;
    public void handleIncomingMessages(int delay) throws Exception;
    public boolean isActive(String nodeName) throws Exception;
    public void pushRelay(String nodeName) throws Exception;
    public void popRelay() throws Exception;
    public boolean exists(String key) throws Exception;
    public String read(String key) throws Exception;
    public boolean write(String key, String value) throws Exception;
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;
}
// DO NOT EDIT ends

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
    private Random random = new Random();

    // For handling timeouts and retries
    private ExecutorService executor = Executors.newCachedThreadPool();

    // Constants
    private static final int MAX_PACKET_SIZE = 1024;
    private static final int REQUEST_TIMEOUT = 5000; // ms
    private static final int MAX_RETRIES = 3;

    // Inner class for address key-value pairs with distance calculation
    private class AddressKeyValuePair {
        String key;
        String value;
        byte[] hashID;
        int distance;
        long lastActivity; // Timestamp of last activity

        AddressKeyValuePair(String key, String value) throws Exception {
            this.key = key;
            this.value = value;
            this.hashID = HashID.computeHashID(key);
            this.distance = calculateDistance(nodeHashID, this.hashID);
            this.lastActivity = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return key + "=" + value;
        }
    }

    // Callback interface for handling asynchronous responses
    private interface ResponseCallback {
        void onResponse(String response);
        void onTimeout();
    }

    @Override
    public void setNodeName(String nodeName) throws Exception {
        if (!nodeName.startsWith("N:")) {
            throw new IllegalArgumentException("Node name must start with N:");
        }
        this.nodeName = nodeName;
        this.nodeHashID = HashID.computeHashID(nodeName);
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        this.port = portNumber;

        // Try to open the specified port
        try {
            this.socket = new DatagramSocket(portNumber);
            System.out.println("Opened socket on port " + portNumber);
        } catch (BindException e) {
            // Port is already in use, try the next few ports
            boolean success = false;
            for (int offset = 1; offset <= 5; offset++) {
                try {
                    this.socket = new DatagramSocket(portNumber + offset);
                    this.port = portNumber + offset; // Update the port we're using
                    System.out.println("Opened socket on port " + this.port + " (original " + portNumber + " was in use)");
                    success = true;
                    break;
                } catch (BindException be) {
                    // Continue trying
                }
            }

            if (!success) {
                throw new Exception("Could not find an available port. Tried ports " +
                        portNumber + " through " + (portNumber + 5));
            }
        }

        // Store our own address key/value pair
        String localIP = InetAddress.getLocalHost().getHostAddress();
        String value = localIP + ":" + this.port; // Use the port we actually bound to
        keyValueStore.put(nodeName, value);

        // Also add to our address map
        try {
            AddressKeyValuePair selfPair = new AddressKeyValuePair(nodeName, value);
            addAddressKeyValuePair(selfPair);
        } catch (Exception e) {
            System.err.println("Error adding self to address map: " + e.getMessage());
        }

        // Start background listener thread
        startListenerThread();
    }

    private void startListenerThread() {
        Thread listenerThread = new Thread(() -> {
            try {
                byte[] buffer = new byte[MAX_PACKET_SIZE];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                while (!socket.isClosed()) {
                    try {
                        // Reset buffer for each packet
                        Arrays.fill(buffer, (byte)0);
                        packet.setLength(buffer.length);

                        // Receive packet
                        socket.receive(packet);

                        // Copy data to prevent modification in next receive
                        byte[] data = Arrays.copyOf(packet.getData(), packet.getLength());
                        final InetAddress senderAddress = packet.getAddress();
                        final int senderPort = packet.getPort();

                        // Process message in a separate thread
                        executor.submit(() -> {
                            try {
                                String message = new String(data, StandardCharsets.UTF_8);
                                processMessage(message, senderAddress, senderPort);
                            } catch (Exception e) {
                                System.err.println("Error processing message: " + e.getMessage());
                            }
                        });
                    } catch (SocketTimeoutException e) {
                        // Ignore timeouts - normal behavior
                    } catch (IOException e) {
                        if (!socket.isClosed()) {
                            System.err.println("Error receiving packet: " + e.getMessage());
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Listener thread error: " + e.getMessage());
            }
        });
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        if (socket == null) {
            throw new IllegalStateException("Socket not initialized. Call openPort first.");
        }

        // If delay is 0, wait indefinitely
        if (delay <= 0) {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            return;
        }

        // For positive delay, wait for that amount of time
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // Process an incoming message
    private void processMessage(String message, InetAddress senderAddress, int senderPort) {
        if (message.length() < 3) {
            // Ignore too short messages
            return;
        }

        try {
            // Check for proper transaction ID format
            if (message.charAt(2) != ' ') {
                System.out.println("Ignoring message with invalid transaction ID format: " + message);
                // Send information message about error
                String txid = message.substring(0, 2);
                sendPacket(txid + " I 4 No space after transaction ID ", senderAddress, senderPort);
                return;
            }

            String txid = message.substring(0, 2);
            char messageType = message.charAt(3);

            // Check if this is a response to a pending transaction
            ResponseCallback callback = pendingTransactions.remove(txid);
            if (callback != null) {
                callback.onResponse(message);
                return;
            }

            // Handle as a request based on message type
            switch (messageType) {
                case 'G': // Name request
                    handleNameRequest(txid, senderAddress, senderPort);
                    break;
                case 'N': // Nearest request
                    if (message.length() > 5) {
                        String hashIDHex = message.substring(5).trim();
                        handleNearestRequest(txid, hashIDHex, senderAddress, senderPort);
                    }
                    break;
                case 'E': // Key existence request
                    if (message.length() > 5) {
                        handleKeyExistenceRequest(txid, message.substring(5), senderAddress, senderPort);
                    }
                    break;
                case 'R': // Read request
                    if (message.length() > 5) {
                        handleReadRequest(txid, message.substring(5), senderAddress, senderPort);
                    }
                    break;
                case 'W': // Write request
                    if (message.length() > 5) {
                        handleWriteRequest(txid, message.substring(5), senderAddress, senderPort);
                    }
                    break;
                case 'C': // Compare and swap request
                    if (message.length() > 5) {
                        handleCASRequest(txid, message.substring(5), senderAddress, senderPort);
                    }
                    break;
                case 'V': // Relay request
                    if (message.length() > 5) {
                        handleRelayRequest(txid, message.substring(5), senderAddress, senderPort);
                    }
                    break;
                case 'I': // Information message - just log it
                    System.out.println("Received information message: " + message);
                    break;
                default:
                    System.out.println("Received unknown message type: " + messageType);
                    break;
            }
        } catch (Exception e) {
            System.err.println("Error handling message: " + e.getMessage());
        }
    }

    // Handle a name request
    private void handleNameRequest(String txid, InetAddress address, int port) {
        try {
            String response = txid + " H " + formatString(nodeName);
            sendPacket(response, address, port);
        } catch (Exception e) {
            System.err.println("Error handling name request: " + e.getMessage());
        }
    }

    // Handle a nearest request
    private void handleNearestRequest(String txid, String hashIDHex, InetAddress address, int port) {
        try {
            // Convert hex string to byte array
            byte[] targetHashID = hexStringToByteArray(hashIDHex);

            // Find the nearest nodes
            List<AddressKeyValuePair> nearestNodes = findNearestNodes(targetHashID, 3);

            // Build response
            StringBuilder response = new StringBuilder(txid + " O ");
            for (AddressKeyValuePair pair : nearestNodes) {
                response.append(formatString(pair.key)).append(formatString(pair.value));
            }

            // Send response
            sendPacket(response.toString(), address, port);
        } catch (Exception e) {
            System.err.println("Error handling nearest request: " + e.getMessage());
        }
    }

    // Handle a key existence request
    private void handleKeyExistenceRequest(String txid, String keyString, InetAddress address, int port) {
        try {
            // Parse the key
            String key = parseString(keyString);

            char responseChar;

            // Check if we have this key
            if (keyValueStore.containsKey(key)) {
                responseChar = 'Y';
            } else {
                // Check if we should be one of the nodes storing this key
                byte[] keyHashID = HashID.computeHashID(key);
                if (shouldStoreKey(keyHashID)) {
                    responseChar = 'N';
                } else {
                    responseChar = '?';
                }
            }

            // Send response
            String response = txid + " F " + responseChar;
            sendPacket(response, address, port);
        } catch (Exception e) {
            System.err.println("Error handling key existence request: " + e.getMessage());
        }
    }

    // Handle a read request
    private void handleReadRequest(String txid, String keyString, InetAddress address, int port) {
        try {
            // Parse the key
            String key = parseString(keyString);

            char responseChar;
            String value = "";

            // Check if we have this key
            if (keyValueStore.containsKey(key)) {
                responseChar = 'Y';
                value = keyValueStore.get(key);
            } else {
                // Check if we should be one of the nodes storing this key
                byte[] keyHashID = HashID.computeHashID(key);
                if (shouldStoreKey(keyHashID)) {
                    responseChar = 'N';
                } else {
                    responseChar = '?';
                }
            }

            // Send response
            String response = txid + " S " + responseChar + " " + formatString(value);
            sendPacket(response, address, port);
        } catch (Exception e) {
            System.err.println("Error handling read request: " + e.getMessage());
        }
    }

    // Handle a write request
    private void handleWriteRequest(String txid, String message, InetAddress address, int port) {
        try {
            // Parse key and value
            int spacePos = message.indexOf(' ');
            if (spacePos == -1) return;

            int keySpaceCount = Integer.parseInt(message.substring(0, spacePos));
            String restAfterCount = message.substring(spacePos + 1);

            // Find where the key ends (after keySpaceCount spaces)
            int spaceCount = 0;
            int keyEndPos = -1;
            for (int i = 0; i < restAfterCount.length(); i++) {
                if (restAfterCount.charAt(i) == ' ') {
                    spaceCount++;
                    if (spaceCount > keySpaceCount) {
                        keyEndPos = i;
                        break;
                    }
                }
            }

            if (keyEndPos == -1) return;

            String key = restAfterCount.substring(0, keyEndPos);
            String valueSection = restAfterCount.substring(keyEndPos + 1);

            // Parse value
            spacePos = valueSection.indexOf(' ');
            if (spacePos == -1) return;

            int valueSpaceCount = Integer.parseInt(valueSection.substring(0, spacePos));
            String valueRestAfterCount = valueSection.substring(spacePos + 1);

            // Find where the value ends
            spaceCount = 0;
            int valueEndPos = -1;
            for (int i = 0; i < valueRestAfterCount.length(); i++) {
                if (valueRestAfterCount.charAt(i) == ' ') {
                    spaceCount++;
                    if (spaceCount > valueSpaceCount) {
                        valueEndPos = i;
                        break;
                    }
                }
            }

            if (valueEndPos == -1) return;

            String value = valueRestAfterCount.substring(0, valueEndPos);

            // Update passive mapping
            if (key.startsWith("N:") && !key.equals(nodeName)) {
                try {
                    // Add node address to our mapping
                    String[] addrParts = value.split(":");
                    if (addrParts.length == 2) {
                        AddressKeyValuePair pair = new AddressKeyValuePair(key, value);
                        addAddressKeyValuePair(pair);
                    }
                } catch (Exception e) {
                    System.err.println("Error adding address key/value pair: " + e.getMessage());
                }
            }

            char responseChar;

            // Check if we already have this key or should store it
            if (keyValueStore.containsKey(key)) {
                keyValueStore.put(key, value);
                responseChar = 'R'; // Replaced
            } else {
                // Check if we should be one of the nodes storing this key
                byte[] keyHashID = HashID.computeHashID(key);
                if (shouldStoreKey(keyHashID)) {
                    keyValueStore.put(key, value);
                    responseChar = 'A'; // Added
                } else {
                    responseChar = 'X'; // Not stored
                }
            }

            // Send response
            String response = txid + " X " + responseChar;
            sendPacket(response, address, port);
        } catch (Exception e) {
            System.err.println("Error handling write request: " + e.getMessage());
        }
    }

    // Handle a compare and swap request
    private void handleCASRequest(String txid, String message, InetAddress address, int port) {
        try {
            // Parse key, expected value, and new value
            int spacePos = message.indexOf(' ');
            if (spacePos == -1) return;

            int keySpaceCount = Integer.parseInt(message.substring(0, spacePos));
            String restAfterKeyCount = message.substring(spacePos + 1);

            // Find where the key ends
            int spaceCount = 0;
            int keyEndPos = -1;
            for (int i = 0; i < restAfterKeyCount.length(); i++) {
                if (restAfterKeyCount.charAt(i) == ' ') {
                    spaceCount++;
                    if (spaceCount > keySpaceCount) {
                        keyEndPos = i;
                        break;
                    }
                }
            }

            if (keyEndPos == -1) return;

            String key = restAfterKeyCount.substring(0, keyEndPos);
            String expectedValueSection = restAfterKeyCount.substring(keyEndPos + 1);

            // Parse expected value
            spacePos = expectedValueSection.indexOf(' ');
            if (spacePos == -1) return;

            int expectedValueSpaceCount = Integer.parseInt(expectedValueSection.substring(0, spacePos));
            String restAfterExpectedCount = expectedValueSection.substring(spacePos + 1);

            // Find where the expected value ends
            spaceCount = 0;
            int expectedValueEndPos = -1;
            for (int i = 0; i < restAfterExpectedCount.length(); i++) {
                if (restAfterExpectedCount.charAt(i) == ' ') {
                    spaceCount++;
                    if (spaceCount > expectedValueSpaceCount) {
                        expectedValueEndPos = i;
                        break;
                    }
                }
            }

            if (expectedValueEndPos == -1) return;

            String expectedValue = restAfterExpectedCount.substring(0, expectedValueEndPos);
            String newValueSection = restAfterExpectedCount.substring(expectedValueEndPos + 1);

            // Parse new value
            spacePos = newValueSection.indexOf(' ');
            if (spacePos == -1) return;

            int newValueSpaceCount = Integer.parseInt(newValueSection.substring(0, spacePos));
            String restAfterNewCount = newValueSection.substring(spacePos + 1);

            // Find where the new value ends
            spaceCount = 0;
            int newValueEndPos = -1;
            for (int i = 0; i < restAfterNewCount.length(); i++) {
                if (restAfterNewCount.charAt(i) == ' ') {
                    spaceCount++;
                    if (spaceCount > newValueSpaceCount) {
                        newValueEndPos = i;
                        break;
                    }
                }
            }

            if (newValueEndPos == -1) return;

            String newValue = restAfterNewCount.substring(0, newValueEndPos);

            char responseChar;

            // Perform the compare and swap atomically
            synchronized (this) {
                if (keyValueStore.containsKey(key)) {
                    String currentValue = keyValueStore.get(key);
                    if (currentValue.equals(expectedValue)) {
                        keyValueStore.put(key, newValue);
                        responseChar = 'R'; // Replaced
                    } else {
                        responseChar = 'N'; // Value did not match
                    }
                } else {
                    // Check if we should be one of the nodes storing this key
                    byte[] keyHashID = HashID.computeHashID(key);
                    if (shouldStoreKey(keyHashID)) {
                        keyValueStore.put(key, newValue);
                        responseChar = 'A'; // Added
                    } else {
                        responseChar = 'X'; // Not stored
                    }
                }
            }

            // Send response
            String response = txid + " D " + responseChar;
            sendPacket(response, address, port);
        } catch (Exception e) {
            System.err.println("Error handling CAS request: " + e.getMessage());
        }
    }

    // Handle a relay request
    private void handleRelayRequest(String txid, String message, InetAddress address, int port) {
        try {
            // Parse target node name
            int spacePos = message.indexOf(' ');
            if (spacePos == -1) return;

            int nodeNameSpaceCount = Integer.parseInt(message.substring(0, spacePos));
            String restAfterCount = message.substring(spacePos + 1);

            // Find where the node name ends
            int spaceCount = 0;
            int nodeNameEndPos = -1;
            for (int i = 0; i < restAfterCount.length(); i++) {
                if (restAfterCount.charAt(i) == ' ') {
                    spaceCount++;
                    if (spaceCount > nodeNameSpaceCount) {
                        nodeNameEndPos = i;
                        break;
                    }
                }
            }

            if (nodeNameEndPos == -1) return;

            String targetNodeName = restAfterCount.substring(0, nodeNameEndPos);
            String innerMessage = restAfterCount.substring(nodeNameEndPos + 1);

            // Extract inner message transaction ID
            if (innerMessage.length() < 2) return;
            String innerTxid = innerMessage.substring(0, 2);

            // Find the target node's address
            String nodeValue = keyValueStore.get(targetNodeName);
            if (nodeValue == null) {
                System.err.println("Cannot relay to unknown node: " + targetNodeName);
                return;
            }

            // Parse address
            String[] parts = nodeValue.split(":");
            if (parts.length != 2) {
                System.err.println("Invalid node address format: " + nodeValue);
                return;
            }

            InetAddress targetAddress = InetAddress.getByName(parts[0]);
            int targetPort = Integer.parseInt(parts[1]);

            // Register callback for the response
            final String finalTxid = txid;
            ResponseCallback callback = new ResponseCallback() {
                @Override
                public void onResponse(String response) {
                    try {
                        // Replace inner transaction ID with relay transaction ID
                        String relayResponse = finalTxid + response.substring(2);
                        sendPacket(relayResponse, address, port);
                    } catch (Exception e) {
                        System.err.println("Error relaying response: " + e.getMessage());
                    }
                }

                @Override
                public void onTimeout() {
                    System.err.println("Timeout on relay to " + targetNodeName);
                }
            };

            // Register callback and forward the message
            pendingTransactions.put(innerTxid, callback);
            sendPacket(innerMessage, targetAddress, targetPort);
        } catch (Exception e) {
            System.err.println("Error handling relay request: " + e.getMessage());
        }
    }

    // Add an address key/value pair
    private synchronized void addAddressKeyValuePair(AddressKeyValuePair pair) {
        if (pair == null || pair.key == null || pair.value == null) {
            return;
        }

        // Store in key-value store
        keyValueStore.put(pair.key, pair.value);

        // Get the list for this distance, or create a new one
        List<AddressKeyValuePair> pairsAtDistance = addressKeyValuesByDistance.computeIfAbsent(
                pair.distance, k -> new ArrayList<>());

        // Check if we already have this node
        boolean exists = false;
        for (int i = 0; i < pairsAtDistance.size(); i++) {
            if (pairsAtDistance.get(i).key.equals(pair.key)) {
                // Update existing entry
                pairsAtDistance.set(i, pair);
                exists = true;
                break;
            }
        }

        if (!exists) {
            // Add new entry
            pairsAtDistance.add(pair);

            // Limit to 3 per distance as per RFC
            if (pairsAtDistance.size() > 3) {
                // Sort by last activity and keep the 3 most recent
                pairsAtDistance.sort(Comparator.comparingLong(p -> -p.lastActivity));
                pairsAtDistance = pairsAtDistance.subList(0, 3);
                addressKeyValuesByDistance.put(pair.distance, pairsAtDistance);
            }
        }
    }

    // Calculate the distance between two hashIDs
    private int calculateDistance(byte[] hash1, byte[] hash2) {
        if (hash1 == null || hash2 == null || hash1.length != hash2.length) {
            return 256; // Maximum distance
        }

        // Find the number of leading identical bits
        int matchingBits = 0;
        for (int i = 0; i < hash1.length; i++) {
            int xorByte = hash1[i] ^ hash2[i];
            if (xorByte == 0) {
                matchingBits += 8;
            } else {
                // Count leading zeros in the XOR result
                int mask = 0x80;
                while ((xorByte & mask) == 0 && mask > 0) {
                    matchingBits++;
                    mask >>= 1;
                }
                break;
            }
        }

        return 256 - matchingBits;
    }

    // Find the nearest nodes to a given hashID
    private List<AddressKeyValuePair> findNearestNodes(byte[] targetHashID, int max) {
        List<AddressKeyValuePair> allPairs = new ArrayList<>();

        // Collect all address pairs
        synchronized (this) {
            for (List<AddressKeyValuePair> pairsList : addressKeyValuesByDistance.values()) {
                for (AddressKeyValuePair pair : pairsList) {
                    try {
                        // Calculate distance to target
                        int distance = calculateDistance(targetHashID, pair.hashID);

                        // Create a new pair with updated distance
                        AddressKeyValuePair newPair = new AddressKeyValuePair(pair.key, pair.value);
                        newPair.distance = distance;
                        allPairs.add(newPair);
                    } catch (Exception e) {
                        System.err.println("Error calculating distance: " + e.getMessage());
                    }
                }
            }
        }

        // Sort by distance
        allPairs.sort(Comparator.comparingInt(p -> p.distance));

        // Return up to max nodes
        return allPairs.size() <= max ? allPairs : allPairs.subList(0, max);
    }

    // Check if this node should store a key
    private boolean shouldStoreKey(byte[] keyHashID) {
        try {
            // Calculate our distance to the key
            int ourDistance = calculateDistance(nodeHashID, keyHashID);

            // Find the three nearest nodes
            List<AddressKeyValuePair> nearestNodes = findNearestNodes(keyHashID, 3);

            // If we know fewer than 3 nodes, we should store it
            if (nearestNodes.size() < 3) {
                return true;
            }

            // Check if we're one of the 3 closest
            for (AddressKeyValuePair pair : nearestNodes) {
                if (ourDistance <= pair.distance && !pair.key.equals(nodeName)) {
                    return true;
                }
            }

            return false;
        } catch (Exception e) {
            System.err.println("Error checking if should store key: " + e.getMessage());
            return false;
        }
    }

    // Parse a CRN-formatted string
    private String parseString(String message) {
        try {
            int spacePos = message.indexOf(' ');
            if (spacePos == -1) return null;

            int spaceCount = Integer.parseInt(message.substring(0, spacePos));
            String content = message.substring(spacePos + 1);

            // Count spaces
            int count = 0;
            int endPos = -1;
            for (int i = 0; i < content.length(); i++) {
                if (content.charAt(i) == ' ') {
                    count++;
                    if (count > spaceCount) {
                        endPos = i;
                        break;
                    }
                }
            }

            if (endPos == -1) return null;
            return content.substring(0, endPos);
        } catch (Exception e) {
            System.err.println("Error parsing string: " + e.getMessage());
            return null;
        }
    }

    // Format a string according to CRN protocol
    private String formatString(String s) {
        int spaceCount = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == ' ') {
                spaceCount++;
            }
        }
        return spaceCount + " " + s + " ";
    }

    // Convert hex string to byte array
    private byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

    // Generate a unique transaction ID
    private String generateTransactionID() {
        byte[] txid = new byte[2];
        do {
            random.nextBytes(txid);
            // Ensure it doesn't contain spaces
        } while (txid[0] == 0x20 || txid[1] == 0x20);

        return new String(txid, StandardCharsets.ISO_8859_1);
    }

    // Send a UDP packet
    private void sendPacket(String message, InetAddress address, int port) throws Exception {
        if (message == null || address == null) {
            return;
        }

        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
        socket.send(packet);
    }

    // Send a request and wait for a response
    private String sendRequest(String request, InetAddress address, int port, int timeoutMs) throws Exception {
        final String[] response = { null };
        final CountDownLatch latch = new CountDownLatch(1);

        String txid = request.substring(0, 2);

        ResponseCallback callback = new ResponseCallback() {
            @Override
            public void onResponse(String resp) {
                response[0] = resp;
                latch.countDown();
            }

            @Override
            public void onTimeout() {
                latch.countDown();
            }
        };

        // Register callback and send request
        pendingTransactions.put(txid, callback);
        sendPacket(request, address, port);

        // Wait for response with timeout
        latch.await(timeoutMs, TimeUnit.MILLISECONDS);

        // If no response received, remove the callback
        pendingTransactions.remove(txid);

        return response[0];
    }

    @Override
    public boolean isActive(String nodeName) throws Exception {
        if (!keyValueStore.containsKey(nodeName)) {
            return false;
        }

        String nodeValue = keyValueStore.get(nodeName);
        String[] parts = nodeValue.split(":");
        if (parts.length != 2) {
            return false;
        }

        InetAddress address = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);

        String txid = generateTransactionID();
        String request = txid + " G";

        String response = sendRequest(request, address, port, REQUEST_TIMEOUT);

        return response != null && response.length() >= 5 && response.charAt(3) == 'H';
    }

    @Override
    public void pushRelay(String nodeName) throws Exception {
        if (!keyValueStore.containsKey(nodeName)) {
            throw new Exception("Unknown relay node: " + nodeName);
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
        // Check local store first
        if (keyValueStore.containsKey(key)) {
            return true;
        }

        // Calculate hash of key
        byte[] keyHash = HashID.computeHashID(key);

        // Find the nodes that should have this key
        List<AddressKeyValuePair> closestNodes = findNearestNodes(keyHash, 3);

        // Try each node
        for (AddressKeyValuePair pair : closestNodes) {
            String[] parts = pair.value.split(":");
            if (parts.length != 2) continue;

            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            // Send through relay if needed
            if (!relayStack.isEmpty()) {
                // Use relay
                boolean result = queryThroughRelay("E", key, null, null, address, port);
                if (result) return true;
            } else {
                // Direct query
                for (int retry = 0; retry < MAX_RETRIES; retry++) {
                    String txid = generateTransactionID();
                    String request = txid + " E " + formatString(key);

                    String response = sendRequest(request, address, port, REQUEST_TIMEOUT);

                    if (response != null && response.length() >= 5 && response.charAt(3) == 'F') {
                        char result = response.charAt(5);
                        if (result == 'Y') {
                            return true;
                        }
                        // If not found but correct node, no need to try others
                        if (result == 'N') {
                            break;
                        }
                    }
                }
            }
        }

        return false;
    }

    @Override
    public String read(String key) throws Exception {
        // Check local store first
        if (keyValueStore.containsKey(key)) {
            return keyValueStore.get(key);
        }

        // Calculate hash of key
        byte[] keyHash = HashID.computeHashID(key);

        // Find nodes that should store this key
        List<AddressKeyValuePair> closestNodes = findNearestNodes(keyHash, 3);

        System.out.println("Reading key: " + key + ", Closest nodes: " + closestNodes);

        // Try each node
        for (AddressKeyValuePair pair : closestNodes) {
            String[] parts = pair.value.split(":");
            if (parts.length != 2) continue;

            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            System.out.println("Trying to read from node: " + pair.key);

            // Send through relay if needed
            if (!relayStack.isEmpty()) {
                // Use relay
                String result = queryThroughRelayForString("R", key, null, null, address, port);
                if (result != null) {
                    keyValueStore.put(key, result);
                    return result;
                }
            } else {
                // Direct query with retries
                for (int retry = 0; retry < MAX_RETRIES; retry++) {
                    String txid = generateTransactionID();
                    String request = txid + " R " + formatString(key);

                    System.out.println("Sending read request to " + pair.key + ": " + txid + " R " + formatString(key));

                    String response = sendRequest(request, address, port, REQUEST_TIMEOUT);

                    if (response != null) {
                        System.out.println("Received response: " + response);
                        if (response.length() >= 7 && response.charAt(3) == 'S') {
                            char result = response.charAt(5);
                            if (result == 'Y') {
                                String value = parseString(response.substring(7));
                                if (value != null) {
                                    keyValueStore.put(key, value);
                                    return value;
                                }
                            } else if (result == 'N') {
                                // Key should be on this node but isn't found
                                break;
                            }
                        }
                    } else {
                        System.out.println("Retry " + (retry + 1) + " for " + pair.key);
                    }
                }
            }
        }

        return null;
    }

    @Override
    public boolean write(String key, String value) throws Exception {
        // Store locally for data keys
        if (key.startsWith("D:")) {
            keyValueStore.put(key, value);
        }

        // Calculate hash of key
        byte[] keyHash = HashID.computeHashID(key);

        // Find nodes that should store this key
        List<AddressKeyValuePair> closestNodes = findNearestNodes(keyHash, 3);

        boolean success = false;

        // Try each node
        for (AddressKeyValuePair pair : closestNodes) {
            String[] parts = pair.value.split(":");
            if (parts.length != 2) continue;

            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            // Send through relay if needed
            if (!relayStack.isEmpty()) {
                // Use relay
                boolean result = queryThroughRelay("W", key, value, null, address, port);
                if (result) {
                    success = true;
                    // Store locally too
                    keyValueStore.put(key, value);
                }
            } else {
                // Direct query with retries
                for (int retry = 0; retry < MAX_RETRIES; retry++) {
                    String txid = generateTransactionID();
                    String request = txid + " W " + formatString(key) + formatString(value);

                    String response = sendRequest(request, address, port, REQUEST_TIMEOUT);

                    if (response != null && response.length() >= 5 && response.charAt(3) == 'X') {
                        char result = response.charAt(5);
                        if (result == 'R' || result == 'A') {
                            success = true;
                            // Store locally too
                            keyValueStore.put(key, value);
                            break;
                        }
                    }
                }
            }
        }

        return success || key.startsWith("D:");
    }

    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        // Check local store first
        if (keyValueStore.containsKey(key)) {
            String localValue = keyValueStore.get(key);
            if (localValue.equals(currentValue)) {
                keyValueStore.put(key, newValue);
                return true;
            }
        }

        // Calculate hash of key
        byte[] keyHash = HashID.computeHashID(key);

        // Find nodes that should store this key
        List<AddressKeyValuePair> closestNodes = findNearestNodes(keyHash, 3);

        boolean success = false;

        // Try each node
        for (AddressKeyValuePair pair : closestNodes) {
            String[] parts = pair.value.split(":");
            if (parts.length != 2) continue;

            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            // Send through relay if needed
            if (!relayStack.isEmpty()) {
                // Use relay
                boolean result = queryThroughRelay("C", key, currentValue, newValue, address, port);
                if (result) {
                    success = true;
                    keyValueStore.put(key, newValue);
                    break;
                }
            } else {
                // Direct query with retries
                for (int retry = 0; retry < MAX_RETRIES; retry++) {
                    String txid = generateTransactionID();
                    String request = txid + " C " + formatString(key) + formatString(currentValue) + formatString(newValue);

                    String response = sendRequest(request, address, port, REQUEST_TIMEOUT);

                    if (response != null && response.length() >= 5 && response.charAt(3) == 'D') {
                        char result = response.charAt(5);
                        if (result == 'R' || result == 'A') {
                            success = true;
                            keyValueStore.put(key, newValue);
                            break;
                        } else if (result == 'N') {
                            // Current value didn't match
                            break;
                        }
                    }
                }
            }
        }

        return success;
    }

    // Helper method to send a query through a relay node
    private boolean queryThroughRelay(String opType, String key, String value1, String value2,
                                      InetAddress destAddress, int destPort) throws Exception {
        if (relayStack.isEmpty()) {
            return false;
        }

        String relayNodeName = relayStack.peek();
        String relayNodeValue = keyValueStore.get(relayNodeName);
        if (relayNodeValue == null) {
            return false;
        }

        String[] parts = relayNodeValue.split(":");
        if (parts.length != 2) {
            return false;
        }

        InetAddress relayAddress = InetAddress.getByName(parts[0]);
        int relayPort = Integer.parseInt(parts[1]);

        // Create the inner message
        String innerTxid = generateTransactionID();
        StringBuilder innerMessageBuilder = new StringBuilder(innerTxid + " " + opType + " ");

        // Add key and values based on operation type
        if (opType.equals("E") || opType.equals("R")) {
            innerMessageBuilder.append(formatString(key));
        } else if (opType.equals("W")) {
            innerMessageBuilder.append(formatString(key)).append(formatString(value1));
        } else if (opType.equals("C")) {
            innerMessageBuilder.append(formatString(key)).append(formatString(value1)).append(formatString(value2));
        }

        String innerMessage = innerMessageBuilder.toString();

        // Create the relay message
        String relayTxid = generateTransactionID();
        String targetName = "";
        try {
            // Get target node name via name request
            String nameRequestTxid = generateTransactionID();
            String nameRequest = nameRequestTxid + " G";

            String nameResponse = sendRequest(nameRequest, destAddress, destPort, REQUEST_TIMEOUT);
            if (nameResponse != null && nameResponse.length() >= 5 && nameResponse.charAt(3) == 'H') {
                targetName = parseString(nameResponse.substring(5));
            }

            if (targetName.isEmpty()) {
                return false;
            }
        } catch (Exception e) {
            System.err.println("Error getting target node name: " + e.getMessage());
            return false;
        }

        String relayMessage = relayTxid + " V " + formatString(targetName) + innerMessage;

        // Send relay message and wait for response
        String response = sendRequest(relayMessage, relayAddress, relayPort, REQUEST_TIMEOUT * 2);

        // Process response based on operation type
        if (response != null && response.length() >= 5) {
            char responseType = response.charAt(3);
            if ((opType.equals("E") && responseType == 'F') ||
                    (opType.equals("R") && responseType == 'S') ||
                    (opType.equals("W") && responseType == 'X') ||
                    (opType.equals("C") && responseType == 'D')) {

                char result = response.charAt(5);
                return result == 'Y' || result == 'R' || result == 'A';
            }
        }

        return false;
    }

    // Helper method to get a string value through a relay
    private String queryThroughRelayForString(String opType, String key, String value1, String value2,
                                              InetAddress destAddress, int destPort) throws Exception {
        if (relayStack.isEmpty() || !opType.equals("R")) {
            return null;
        }

        String relayNodeName = relayStack.peek();
        String relayNodeValue = keyValueStore.get(relayNodeName);
        if (relayNodeValue == null) {
            return null;
        }

        String[] parts = relayNodeValue.split(":");
        if (parts.length != 2) {
            return null;
        }

        InetAddress relayAddress = InetAddress.getByName(parts[0]);
        int relayPort = Integer.parseInt(parts[1]);

        // Create the inner message
        String innerTxid = generateTransactionID();
        String innerMessage = innerTxid + " R " + formatString(key);

        // Get target node name
        String targetName = "";
        try {
            String nameRequestTxid = generateTransactionID();
            String nameRequest = nameRequestTxid + " G";

            String nameResponse = sendRequest(nameRequest, destAddress, destPort, REQUEST_TIMEOUT);
            if (nameResponse != null && nameResponse.length() >= 5 && nameResponse.charAt(3) == 'H') {
                targetName = parseString(nameResponse.substring(5));
            }

            if (targetName.isEmpty()) {
                return null;
            }
        } catch (Exception e) {
            System.err.println("Error getting target node name: " + e.getMessage());
            return null;
        }

        // Create the relay message
        String relayTxid = generateTransactionID();
        String relayMessage = relayTxid + " V " + formatString(targetName) + innerMessage;

        // Send relay message and wait for response
        String response = sendRequest(relayMessage, relayAddress, relayPort, REQUEST_TIMEOUT * 2);

        // Process response
        if (response != null && response.length() >= 7 && response.charAt(3) == 'S') {
            char result = response.charAt(5);
            if (result == 'Y') {
                return parseString(response.substring(7));
            }
        }

        return null;
    }
}