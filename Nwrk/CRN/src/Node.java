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
    private Thread listenerThread;

    // Constants
    private static final int MAX_PACKET_SIZE = 1024;
    private static final int REQUEST_TIMEOUT = 3000; // ms
    private static final int MAX_RETRIES = 3;

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
        }

        @Override
        public String toString() {
            return name + " (" + address.getHostAddress() + ":" + port + ")";
        }
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

        // Try to open the specified port, fallback to next available if needed
        try {
            this.socket = new DatagramSocket(portNumber);
        } catch (BindException e) {
            boolean success = false;
            for (int offset = 1; offset <= 5; offset++) {
                try {
                    this.socket = new DatagramSocket(portNumber + offset);
                    this.port = portNumber + offset;
                    success = true;
                    break;
                } catch (BindException be) {
                    // Continue trying
                }
            }
            if (!success) {
                throw new Exception("Could not bind to ports " + portNumber + " to " + (portNumber + 5));
            }
        }

        // Store our own address
        String localIP = InetAddress.getLocalHost().getHostAddress();
        String value = localIP + ":" + this.port;
        keyValueStore.put(nodeName, value);
        AddressKeyValuePair selfPair = new AddressKeyValuePair(nodeName, value);
        addAddressKeyValuePair(selfPair);

        // Start listener thread
        startListenerThread();
    }

    private void startListenerThread() {
        listenerThread = new Thread(() -> {
            try {
                while (!socket.isClosed()) {
                    byte[] buffer = new byte[MAX_PACKET_SIZE];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);
                    String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);

                    executor.submit(() -> {
                        try {
                            handleBootstrapMessage(message, packet.getAddress(), packet.getPort());
                            processMessage(packet);
                        } catch (Exception e) {
                            // Silent error handling
                        }
                    });
                }
            } catch (IOException e) {
                if (!socket.isClosed()) {
                    System.err.println("Listener error: " + e.getMessage());
                }
            }
        });
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    private void handleBootstrapMessage(String message, InetAddress sourceAddress, int sourcePort) {
        if (message.contains("W") && message.contains("N:")) {
            String[] parts = message.split(" ");
            if (parts.length >= 6 && parts[1].equals("W")) {
                try {
                    int keySpaceCount = Integer.parseInt(parts[2]);
                    String key = parts[3];
                    if (!key.startsWith("N:")) return;

                    int valueSpaceCount = Integer.parseInt(parts[4]);
                    String value = parts[5];
                    if (!value.contains(":")) return;
                    String[] addrParts = value.split(":");
                    if (addrParts.length != 2) return;

                    InetAddress addr = InetAddress.getByName(addrParts[0]);
                    int port = Integer.parseInt(addrParts[1]);
                    NodeAddress nodeAddr = new NodeAddress(key, addr, port);
                    addNodeAddress(key, nodeAddr);
                    keyValueStore.put(key, value);
                    System.out.println("Bootstrap added: " + key + " -> " + value);
                } catch (Exception e) {
                    System.err.println("Failed to parse bootstrap: " + message);
                }
            }
        }
    }

    private void addNodeAddress(String nodeName, NodeAddress nodeAddr) {
        try {
            AddressKeyValuePair pair = new AddressKeyValuePair(nodeName,
                    nodeAddr.address.getHostAddress() + ":" + nodeAddr.port);
            addAddressKeyValuePair(pair);
        } catch (Exception e) {
            // Silent error handling
        }
    }

    private void addAddressKeyValuePair(AddressKeyValuePair pair) {
        keyValueStore.put(pair.key, pair.value);
        List<AddressKeyValuePair> pairsAtDistance = addressKeyValuesByDistance.computeIfAbsent(
                pair.distance, k -> new CopyOnWriteArrayList<>());

        boolean exists = false;
        for (int i = 0; i < pairsAtDistance.size(); i++) {
            if (pairsAtDistance.get(i).key.equals(pair.key)) {
                pairsAtDistance.set(i, pair);
                exists = true;
                break;
            }
        }

        if (!exists) {
            pairsAtDistance.add(pair);
            if (pairsAtDistance.size() > 3) {
                pairsAtDistance.remove(pairsAtDistance.size() - 1);
            }
        }
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        if (socket == null) throw new IllegalStateException("Socket not initialized.");
        Thread.sleep(delay <= 0 ? 10000 : delay);
    }

    private void processMessage(DatagramPacket packet) {
        String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
        if (message.length() < 4) return;

        try {
            String txid = message.substring(0, 2);
            char messageType = message.charAt(3);

            ResponseCallback callback = pendingTransactions.remove(txid);
            if (callback != null) {
                callback.onResponse(message);
                return;
            }

            switch (messageType) {
                case 'G': handleNameRequest(txid, packet.getAddress(), packet.getPort()); break;
                case 'N': if (message.length() > 5) handleNearestRequest(txid, message.substring(5), packet.getAddress(), packet.getPort()); break;
                case 'E': if (message.length() > 5) handleKeyExistenceRequest(txid, message.substring(5), packet.getAddress(), packet.getPort()); break;
                case 'R': if (message.length() > 5) handleReadRequest(txid, message.substring(5), packet.getAddress(), packet.getPort()); break;
                case 'W': if (message.length() > 5) handleWriteRequest(txid, message.substring(5), packet.getAddress(), packet.getPort()); break;
                case 'C': if (message.length() > 5) handleCASRequest(txid, message.substring(5), packet.getAddress(), packet.getPort()); break;
                case 'V': if (message.length() > 5) handleRelayRequest(txid, message.substring(5), packet.getAddress(), packet.getPort()); break;
                case 'I': break; // Ignore info messages
            }
        } catch (Exception e) {
            // Silent error handling
        }
    }

    private void handleNameRequest(String txid, InetAddress address, int port) throws Exception {
        String response = txid + " H " + formatString(nodeName);
        sendPacket(response, address, port);
    }

    private void handleNearestRequest(String txid, String hashIDHex, InetAddress address, int port) throws Exception {
        byte[] targetHashID = hexStringToByteArray(hashIDHex.trim());
        List<AddressKeyValuePair> nearestNodes = findNearestNodes(targetHashID, 3);
        StringBuilder response = new StringBuilder(txid + " O ");
        for (AddressKeyValuePair pair : nearestNodes) {
            response.append(formatKeyValuePair(pair.key, pair.value));
        }
        sendPacket(response.toString(), address, port);
    }

    private void handleKeyExistenceRequest(String txid, String keyString, InetAddress address, int port) throws Exception {
        String key = parseString(keyString);
        if (key == null) return;

        char responseChar;
        if (keyValueStore.containsKey(key)) {
            responseChar = 'Y';
        } else {
            byte[] keyHashID = HashID.computeHashID(key);
            responseChar = shouldStoreKey(keyHashID) ? 'N' : '?';
        }
        String response = txid + " F " + responseChar;
        sendPacket(response, address, port);
    }

    private void handleReadRequest(String txid, String keyString, InetAddress address, int port) throws Exception {
        String key = parseString(keyString);
        if (key == null) return;

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
        System.out.println("Responding to read request for " + key + ": " + response);
        sendPacket(response, address, port);
    }

    private void handleWriteRequest(String txid, String message, InetAddress address, int port) throws Exception {
        String[] parts = message.split(" ", 3);
        if (parts.length < 3) return;

        int keySpaceCount = Integer.parseInt(parts[0]);
        String key = parts[1];
        if (countSpaces(key) != keySpaceCount) return;

        String[] valueParts = parts[2].split(" ", 2);
        if (valueParts.length < 2) return;

        int valueSpaceCount = Integer.parseInt(valueParts[0]);
        String value = valueParts[1];
        if (countSpaces(value) != valueSpaceCount) return;

        char responseChar;

        if (key.startsWith("N:") && !key.equals(nodeName)) {
            String[] addrParts = value.split(":");
            if (addrParts.length == 2) {
                AddressKeyValuePair pair = new AddressKeyValuePair(key, value);
                addAddressKeyValuePair(pair);
            }
        }

        if (keyValueStore.containsKey(key)) {
            keyValueStore.put(key, value);
            responseChar = 'R';
        } else if (key.startsWith("D:") || address.isLoopbackAddress()) {
            keyValueStore.put(key, value);
            responseChar = 'A';
        } else {
            byte[] keyHashID = HashID.computeHashID(key);
            responseChar = shouldStoreKey(keyHashID) ? 'A' : 'X';
            if (responseChar == 'A') keyValueStore.put(key, value);
        }

        String response = txid + " X " + responseChar;
        sendPacket(response, address, port);
    }

    private void handleCASRequest(String txid, String message, InetAddress address, int port) throws Exception {
        int firstSpace = message.indexOf(' ');
        if (firstSpace == -1) return;

        int keyCount = Integer.parseInt(message.substring(0, firstSpace));
        String rest = message.substring(firstSpace + 1);
        int keyEnd = findNthSpace(rest, keyCount);
        if (keyEnd == -1) return;

        String key = rest.substring(0, keyEnd);
        String expectedAndNew = rest.substring(keyEnd + 1);

        firstSpace = expectedAndNew.indexOf(' ');
        if (firstSpace == -1) return;

        int expectedCount = Integer.parseInt(expectedAndNew.substring(0, firstSpace));
        rest = expectedAndNew.substring(firstSpace + 1);
        int expectedEnd = findNthSpace(rest, expectedCount);
        if (expectedEnd == -1) return;

        String expectedValue = rest.substring(0, expectedEnd);
        String newValuePart = rest.substring(expectedEnd + 1);

        firstSpace = newValuePart.indexOf(' ');
        if (firstSpace == -1) return;

        String newValue = newValuePart.substring(firstSpace + 1);

        char responseChar;

        if (keyValueStore.containsKey(key)) {
            String currentValue = keyValueStore.get(key);
            if (currentValue.equals(expectedValue)) {
                keyValueStore.put(key, newValue);
                responseChar = 'R';
            } else {
                responseChar = 'N';
            }
        } else {
            byte[] keyHashID = HashID.computeHashID(key);
            if (shouldStoreKey(keyHashID)) {
                keyValueStore.put(key, newValue);
                responseChar = 'A';
            } else {
                responseChar = 'X';
            }
        }

        String response = txid + " D " + responseChar;
        sendPacket(response, address, port);
    }

    private void handleRelayRequest(String txid, String message, InetAddress address, int port) throws Exception {
        String destNodeName = parseString(message);
        if (destNodeName == null) return;

        int destNameEndPos = message.indexOf(' ', message.indexOf(' ') + 1) + 1;
        String innerMessage = message.substring(destNameEndPos);
        String innerTxid = innerMessage.substring(0, 2);

        String nodeValue = keyValueStore.get(destNodeName);
        if (nodeValue == null) return;

        String[] parts = nodeValue.split(":");
        if (parts.length != 2) return;

        InetAddress destAddress = InetAddress.getByName(parts[0]);
        int destPort = Integer.parseInt(parts[1]);

        ResponseCallback callback = new ResponseCallback() {
            @Override
            public void onResponse(String response) {
                try {
                    String relayResponse = txid + response.substring(2);
                    sendPacket(relayResponse, address, port);
                } catch (Exception e) {
                    // Silent error handling
                }
            }

            @Override
            public void onTimeout() {
                // Silent timeout handling
            }
        };

        pendingTransactions.put(innerTxid, callback);
        sendPacket(innerMessage, destAddress, destPort);
    }

    private int calculateDistance(byte[] hash1, byte[] hash2) {
        if (hash1 == null || hash2 == null || hash1.length != hash2.length) return 256;
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
        return 256 - matchingBits;
    }

    private List<AddressKeyValuePair> findNearestNodes(byte[] targetHashID, int max) {
        List<AddressKeyValuePair> allPairs = new ArrayList<>();
        for (List<AddressKeyValuePair> pairs : addressKeyValuesByDistance.values()) {
            for (AddressKeyValuePair pair : pairs) {
                try {
                    AddressKeyValuePair newPair = new AddressKeyValuePair(pair.key, pair.value);
                    newPair.distance = calculateDistance(targetHashID, pair.hashID);
                    allPairs.add(newPair);
                } catch (Exception e) {
                    // Skip invalid pairs
                }
            }
        }
        Collections.sort(allPairs, Comparator.comparingInt(p -> p.distance));
        return allPairs.size() <= max ? allPairs : allPairs.subList(0, max);
    }

    private boolean shouldStoreKey(byte[] keyHashID) throws Exception {
        List<AddressKeyValuePair> nearestNodes = findNearestNodes(keyHashID, 3);
        int ourDistance = calculateDistance(nodeHashID, keyHashID);
        if (nearestNodes.size() < 3) return true;
        for (AddressKeyValuePair pair : nearestNodes) {
            if (ourDistance < pair.distance) return true;
        }
        return false;
    }

    private String parseString(String message) {
        try {
            int spaceIndex = message.indexOf(' ');
            if (spaceIndex == -1) return null;
            int count = Integer.parseInt(message.substring(0, spaceIndex));
            String content = message.substring(spaceIndex + 1);
            int endIndex = findNthSpace(content, count);
            return endIndex == -1 ? null : content.substring(0, endIndex);
        } catch (Exception e) {
            return null;
        }
    }

    private int findNthSpace(String s, int n) {
        int spaceCount = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == ' ') {
                spaceCount++;
                if (spaceCount > n) return i;
            }
        }
        return -1;
    }

    private String formatString(String s) {
        int spaceCount = countSpaces(s);
        return spaceCount + " " + s + " ";
    }

    private String formatKeyValuePair(String key, String value) {
        return formatString(key) + formatString(value);
    }

    private int countSpaces(String s) {
        return (int) s.chars().filter(ch -> ch == ' ').count();
    }

    private byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) result.append(String.format("%02x", b));
        return result.toString();
    }

    private String generateTransactionID() {
        byte[] txid = new byte[2];
        do {
            random.nextBytes(txid);
        } while (txid[0] <= 0x20 || txid[1] <= 0x20);
        return new String(txid, StandardCharsets.ISO_8859_1);
    }

    private void sendPacket(String message, InetAddress address, int port) throws Exception {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        socket.send(new DatagramPacket(data, data.length, address, port));
    }

    @Override
    public boolean isActive(String nodeName) throws Exception {
        String nodeValue = keyValueStore.get(nodeName);
        if (nodeValue == null) return false;

        String[] parts = nodeValue.split(":");
        if (parts.length != 2) return false;

        InetAddress address = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);

        final boolean[] isActive = {false};
        final CountDownLatch latch = new CountDownLatch(1);

        String txid = generateTransactionID();
        String nameRequest = txid + " G";

        ResponseCallback callback = new ResponseCallback() {
            @Override
            public void onResponse(String response) {
                if (response.length() >= 5 && response.charAt(3) == 'H') isActive[0] = true;
                latch.countDown();
            }

            @Override
            public void onTimeout() {
                latch.countDown();
            }
        };

        pendingTransactions.put(txid, callback);
        sendPacket(nameRequest, address, port);
        latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        return isActive[0];
    }

    @Override
    public void pushRelay(String nodeName) throws Exception {
        if (!keyValueStore.containsKey(nodeName)) throw new Exception("Unknown relay node: " + nodeName);
        relayStack.push(nodeName);
    }

    @Override
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) relayStack.pop();
    }

    @Override
    public boolean exists(String key) throws Exception {
        if (keyValueStore.containsKey(key)) return true;

        byte[] keyHash = HashID.computeHashID(key);
        List<String> closestNodes = findClosestNodeNames(keyHash);

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

            ResponseCallback callback = new ResponseCallback() {
                @Override
                public void onResponse(String response) {
                    if (response.length() >= 5 && response.charAt(3) == 'F') {
                        exists[0] = response.charAt(5) == 'Y';
                    }
                    latch.countDown();
                }

                @Override
                public void onTimeout() {
                    latch.countDown();
                }
            };

            pendingTransactions.put(txid, callback);
            sendPacket(existsRequest, address, port);
            latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);

            if (exists[0]) return true;
        }
        return false;
    }

    private List<String> findClosestNodeNames(byte[] keyHash) {
        List<AddressKeyValuePair> pairs = findNearestNodes(keyHash, 3);
        List<String> nodeNames = new ArrayList<>();
        for (AddressKeyValuePair pair : pairs) nodeNames.add(pair.key);
        System.out.println("Closest nodes for hash " + bytesToHex(keyHash) + ": " + nodeNames);
        return nodeNames;
    }

    private List<NodeAddress> queryNearestNodes(byte[] keyHash, NodeAddress node) throws Exception {
        List<NodeAddress> nearestNodes = new ArrayList<>();
        String txid = generateTransactionID();
        String nearestRequest = txid + " N " + bytesToHex(keyHash);

        final CountDownLatch latch = new CountDownLatch(1);
        final List<String> nodeResponses = new ArrayList<>();

        ResponseCallback callback = new ResponseCallback() {
            @Override
            public void onResponse(String response) {
                if (response.length() >= 5 && response.charAt(3) == 'O') {
                    String nodeList = response.substring(5).trim();
                    String[] parts = nodeList.split("(?<=\\d) (?=[0-9])");
                    for (String part : parts) {
                        String nodeName = parseString(part);
                        if (nodeName != null && nodeName.startsWith("N:")) nodeResponses.add(nodeName);
                    }
                }
                latch.countDown();
            }

            @Override
            public void onTimeout() {
                System.out.println("Timeout querying nearest from " + node.name);
                latch.countDown();
            }
        };

        System.out.println("Querying nearest from " + node.name + " for " + bytesToHex(keyHash));
        pendingTransactions.put(txid, callback);
        sendPacket(nearestRequest, node.address, node.port);

        latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);

        for (String nodeName : nodeResponses) {
            String nodeValue = keyValueStore.get(nodeName);
            if (nodeValue != null) {
                String[] parts = nodeValue.split(":");
                if (parts.length == 2) {
                    InetAddress addr = InetAddress.getByName(parts[0]);
                    int port = Integer.parseInt(parts[1]);
                    nearestNodes.add(new NodeAddress(nodeName, addr, port));
                }
            }
        }
        System.out.println("Nearest nodes from " + node.name + ": " + nearestNodes);
        return nearestNodes;
    }

    @Override
    public String read(String key) throws Exception {
        if (keyValueStore.containsKey(key)) return keyValueStore.get(key);

        byte[] keyHash = HashID.computeHashID(key);
        List<String> initialClosestNodes = findClosestNodeNames(keyHash);

        Set<String> queriedNodes = new HashSet<>();
        Queue<NodeAddress> nodesToQuery = new LinkedList<>();

        for (String nodeName : initialClosestNodes) {
            String nodeValue = keyValueStore.get(nodeName);
            if (nodeValue == null) continue;
            String[] parts = nodeValue.split(":");
            if (parts.length != 2) continue;
            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);
            nodesToQuery.add(new NodeAddress(nodeName, address, port));
        }

        int maxAttempts = 10;
        int attempts = 0;

        while (!nodesToQuery.isEmpty() && attempts < maxAttempts) {
            NodeAddress node = nodesToQuery.poll();
            if (queriedNodes.contains(node.name)) continue;
            queriedNodes.add(node.name);
            attempts++;

            for (int retry = 0; retry < MAX_RETRIES; retry++) {
                final String[] value = {null};
                final char[] responseChar = {' '};
                final CountDownLatch latch = new CountDownLatch(1);

                String txid = generateTransactionID();
                String readRequest = txid + " R " + formatString(key);

                System.out.println("Read request to " + node.name + " (attempt " + attempts + "/" + maxAttempts + ", retry " + (retry + 1) + "): " + readRequest);

                ResponseCallback callback = new ResponseCallback() {
                    @Override
                    public void onResponse(String response) {
                        System.out.println("Response from " + node.name + ": " + response);
                        if (response.length() >= 5 && response.charAt(3) == 'S') {
                            responseChar[0] = response.charAt(5);
                            if (responseChar[0] == 'Y' && response.length() > 7) {
                                value[0] = parseString(response.substring(7));
                            }
                        }
                        latch.countDown();
                    }

                    @Override
                    public void onTimeout() {
                        System.out.println("Timeout from " + node.name + ": " + readRequest);
                        latch.countDown();
                    }
                };

                pendingTransactions.put(txid, callback);
                sendPacket(readRequest, node.address, node.port);

                if (latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS)) {
                    if (value[0] != null) {
                        keyValueStore.put(key, value[0]);
                        System.out.println("Found " + key + " at " + node.name + ": " + value[0]);
                        return value[0];
                    } else if (responseChar[0] == '?' || responseChar[0] == ' ') {
                        System.out.println(node.name + " doesn’t have " + key + ", expanding search");
                        List<NodeAddress> nearestNodes = queryNearestNodes(keyHash, node);
                        nodesToQuery.addAll(nearestNodes);
                        break;
                    }
                } else {
                    System.out.println("No response from " + node.name + ", expanding search");
                    List<NodeAddress> nearestNodes = queryNearestNodes(keyHash, node);
                    nodesToQuery.addAll(nearestNodes);
                    break;
                }
            }
        }

        System.out.println("Failed to find " + key + " after " + attempts + " attempts across " + queriedNodes.size() + " nodes");
        return null;
    }

    @Override
    public boolean write(String key, String value) throws Exception {
        if (key.startsWith("D:")) {
            keyValueStore.put(key, value);
            return true;
        }

        byte[] keyHash = HashID.computeHashID(key);
        List<String> closestNodes = findClosestNodeNames(keyHash);
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

            ResponseCallback callback = new ResponseCallback() {
                @Override
                public void onResponse(String response) {
                    if (response.length() >= 5 && response.charAt(3) == 'X') {
                        char result = response.charAt(5);
                        writeSuccess[0] = (result == 'R' || result == 'A');
                    }
                    latch.countDown();
                }

                @Override
                public void onTimeout() {
                    latch.countDown();
                }
            };

            pendingTransactions.put(txid, callback);
            sendPacket(writeRequest, address, port);
            latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);

            if (writeSuccess[0]) {
                success = true;
                keyValueStore.put(key, value);
            }
        }
        return success || key.startsWith("D:");
    }

    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        if (keyValueStore.containsKey(key)) {
            String localValue = keyValueStore.get(key);
            if (localValue.equals(currentValue)) {
                keyValueStore.put(key, newValue);
                return true;
            }
        }

        byte[] keyHash = HashID.computeHashID(key);
        List<String> closestNodes = findClosestNodeNames(keyHash);
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

            ResponseCallback callback = new ResponseCallback() {
                @Override
                public void onResponse(String response) {
                    if (response.length() >= 5 && response.charAt(3) == 'D') {
                        casSuccess[0] = (response.charAt(5) == 'R' || response.charAt(5) == 'A');
                    }
                    latch.countDown();
                }

                @Override
                public void onTimeout() {
                    latch.countDown();
                }
            };

            pendingTransactions.put(txid, callback);
            sendPacket(casRequest, address, port);
            latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);

            if (casSuccess[0]) {
                success = true;
                keyValueStore.put(key, newValue);
                break;
            }
        }
        return success;
    }
}