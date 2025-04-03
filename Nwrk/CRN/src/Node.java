// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  YOUR_NAME_GOES_HERE
//  YOUR_STUDENT_ID_NUMBER_GOES_HERE
//  YOUR_EMAIL_GOES_HERE

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.security.MessageDigest;

// NodeInterface defining the contract for Node implementation
interface NodeInterface {

    /* Configuration methods */
    // Set the name of the node, must be called before use
    public void setNodeName(String nodeName) throws Exception;

    // Open a UDP port for communication, must be called before use
    public void openPort(int portNumber) throws Exception;

    /* Network usage methods */
    // Handle incoming messages, waiting up to 'delay' milliseconds if > 0, or indefinitely if 0
    public void handleIncomingMessages(int delay) throws Exception;

    // Check if a node is active and responding
    public boolean isActive(String nodeName) throws Exception;

    // Add a relay node to the stack for message routing
    public void pushRelay(String nodeName) throws Exception;

    // Remove the top relay node from the stack
    public void popRelay() throws Exception;

    /* CRN-25 network functionality */
    // Check if a key exists in the network
    public boolean exists(String key) throws Exception;

    // Read the value associated with a key, or null if not found
    public String read(String key) throws Exception;

    // Write a key-value pair to the network
    public boolean write(String key, String value) throws Exception;

    // Compare and swap a key's value if it matches the current value
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;
}

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

    // Thread management
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ExecutorService executor = Executors.newCachedThreadPool();
    private Thread listenerThread;

    // Constants
    private static final int MAX_PACKET_SIZE = 1024;
    private static final int REQUEST_TIMEOUT = 5000; // 5 seconds per RFC
    private static final int MAX_RETRIES = 3;

    // Inner class for address key-value pairs with distance
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

    // Callback interface for asynchronous responses
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
    }

    // HashID computation utility
    private static class HashID {
        static byte[] computeHashID(String input) throws Exception {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(input.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void setNodeName(String nodeName) throws Exception {
        if (!nodeName.startsWith("N:")) {
            throw new IllegalArgumentException("Node name must start with 'N:'");
        }
        this.nodeName = nodeName;
        this.nodeHashID = HashID.computeHashID(nodeName);
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        this.port = portNumber;
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
                } catch (BindException ignored) {}
            }
            if (!success) {
                throw new Exception("Could not bind to ports " + portNumber + " to " + (portNumber + 5));
            }
        }

        String localIP = InetAddress.getLocalHost().getHostAddress();
        String value = localIP + ":" + this.port;
        keyValueStore.put(nodeName, value);

        AddressKeyValuePair selfPair = new AddressKeyValuePair(nodeName, value);
        addAddressKeyValuePair(selfPair);

        startListenerThread();
    }

    private void startListenerThread() {
        listenerThread = new Thread(() -> {
            while (!socket.isClosed()) {
                byte[] buffer = new byte[MAX_PACKET_SIZE];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                try {
                    socket.receive(packet);
                    String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                    executor.submit(() -> {
                        try {
                            handleBootstrapMessage(message, packet.getAddress(), packet.getPort());
                            processMessage(packet);
                        } catch (Exception ignored) {}
                    });
                } catch (IOException e) {
                    if (!socket.isClosed()) {}
                }
            }
        });
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    private void handleBootstrapMessage(String message, InetAddress sourceAddress, int sourcePort) {
        if (message.contains("N:") && message.contains(":20")) {
            String[] parts = message.split(" ");
            for (int i = 0; i < parts.length - 3; i++) {
                if (i + 3 < parts.length && parts[i].startsWith("N:") && parts[i + 2].contains(":")) {
                    String nodeName = parts[i];
                    String[] addrParts = parts[i + 2].split(":");
                    String ip = addrParts[0];
                    int port = Integer.parseInt(addrParts[1].replace("!", ""));
                    try {
                        InetAddress addr = InetAddress.getByName(ip);
                        NodeAddress nodeAddr = new NodeAddress(nodeName, addr, port);
                        addNodeAddress(nodeName, nodeAddr);
                        keyValueStore.put(nodeName, ip + ":" + port);
                    } catch (Exception ignored) {}
                }
            }
        }
    }

    private void addNodeAddress(String nodeName, NodeAddress nodeAddr) {
        try {
            AddressKeyValuePair pair = new AddressKeyValuePair(nodeName,
                    nodeAddr.address.getHostAddress() + ":" + nodeAddr.port);
            addAddressKeyValuePair(pair);
        } catch (Exception ignored) {}
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
        if (socket == null) {
            throw new IllegalStateException("Socket not initialized");
        }
        if (delay <= 0) {
            try {
                while (!socket.isClosed()) {
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            Thread.sleep(delay);
        }
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
        } catch (Exception ignored) {}
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
        } else {
            byte[] keyHashID = HashID.computeHashID(key);
            if (shouldStoreKey(keyHashID)) {
                keyValueStore.put(key, value);
                responseChar = 'A';
            } else {
                responseChar = 'X';
            }
        }
        String response = txid + " X " + responseChar;
        sendPacket(response, address, port);
    }

    private void handleCASRequest(String txid, String message, InetAddress address, int port) throws Exception {
        int firstSpace = message.indexOf(' ');
        if (firstSpace == -1) return;
        int keyCount = Integer.parseInt(message.substring(0, firstSpace));
        String rest = message.substring(firstSpace + 1);
        int keyEnd = findNthSpace(rest, keyCount + 1);
        if (keyEnd == -1) return;
        String key = rest.substring(0, keyEnd);
        String expectedAndNew = rest.substring(keyEnd + 1);
        firstSpace = expectedAndNew.indexOf(' ');
        if (firstSpace == -1) return;
        int expectedCount = Integer.parseInt(expectedAndNew.substring(0, firstSpace));
        rest = expectedAndNew.substring(firstSpace + 1);
        int expectedEnd = findNthSpace(rest, expectedCount + 1);
        if (expectedEnd == -1) return;
        String expectedValue = rest.substring(0, expectedEnd);
        String newValuePart = rest.substring(expectedEnd + 1);
        firstSpace = newValuePart.indexOf(' ');
        if (firstSpace == -1) return;
        String newValue = newValuePart.substring(firstSpace + 1);

        char responseChar;
        if (keyValueStore.containsKey(key)) {
            String currentValue = keyValueStore.get(key);
            responseChar = currentValue.equals(expectedValue) ? 'R' : 'N';
            if (responseChar == 'R') keyValueStore.put(key, newValue);
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
                } catch (Exception ignored) {}
            }
            @Override
            public void onTimeout() {}
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
                } catch (Exception ignored) {}
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
            int endIndex = findNthSpace(content, count + 1);
            if (endIndex == -1) return null;
            return content.substring(0, endIndex);
        } catch (Exception e) {
            return null;
        }
    }

    private int findNthSpace(String str, int n) {
        int count = 0;
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == ' ') {
                count++;
                if (count == n) return i;
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
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }

    private String generateTransactionID() {
        byte[] txid = new byte[2];
        do {
            random.nextBytes(txid);
        } while (txid[0] == 0x20 || txid[1] == 0x20);
        return new String(txid, StandardCharsets.ISO_8859_1);
    }

    private void sendPacket(String message, InetAddress address, int port) throws Exception {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
        socket.send(packet);
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
                if (response.length() >= 5 && response.charAt(3) == 'H') {
                    isActive[0] = true;
                }
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
        if (!keyValueStore.containsKey(nodeName)) {
            throw new Exception("Unknown relay node: " + nodeName);
        }
        relayStack.push(nodeName);
    }

    @Override
    public void popRelay() {
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
        List<String> nodeNames = new ArrayList<>();
        List<AddressKeyValuePair> pairs = findNearestNodes(keyHash, 3);
        for (AddressKeyValuePair pair : pairs) {
            nodeNames.add(pair.key);
        }
        return nodeNames;
    }

    @Override
    public String read(String key) throws Exception {
        if (keyValueStore.containsKey(key)) return keyValueStore.get(key);
        byte[] keyHash = HashID.computeHashID(key);
        Set<String> visited = new HashSet<>();
        Queue<String> nodesToQuery = new LinkedList<>(findClosestNodeNames(keyHash));

        while (!nodesToQuery.isEmpty()) {
            String nodeName = nodesToQuery.poll();
            if (visited.contains(nodeName)) continue;
            visited.add(nodeName);
            String nodeValue = keyValueStore.get(nodeName);
            if (nodeValue == null) continue;
            String[] parts = nodeValue.split(":");
            if (parts.length != 2) continue;
            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            String txid = generateTransactionID();
            String readRequest = txid + " R " + formatString(key);
            final String[] value = {null};
            final char[] status = {' '};
            final CountDownLatch latch = new CountDownLatch(1);

            ResponseCallback callback = new ResponseCallback() {
                @Override
                public void onResponse(String response) {
                    if (response.length() >= 5 && response.charAt(3) == 'S') {
                        status[0] = response.charAt(5);
                        if (status[0] == 'Y' && response.length() > 7) {
                            value[0] = parseString(response.substring(7));
                        }
                    }
                    latch.countDown();
                }
                @Override
                public void onTimeout() {
                    latch.countDown();
                }
            };

            pendingTransactions.put(txid, callback);
            sendPacket(readRequest, address, port);
            latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);

            if (value[0] != null) {
                keyValueStore.put(key, value[0]);
                return value[0];
            } else if (status[0] == '?') {
                String nearestTxid = generateTransactionID();
                String nearestRequest = nearestTxid + " N " + bytesToHex(keyHash);
                final List<String> newNodes = new ArrayList<>();
                final CountDownLatch nearestLatch = new CountDownLatch(1);

                ResponseCallback nearestCallback = new ResponseCallback() {
                    @Override
                    public void onResponse(String response) {
                        if (response.length() >= 5 && response.charAt(3) == 'O') {
                            String[] parts = response.substring(5).trim().split(" 0 ");
                            for (String part : parts) {
                                String[] kv = parseKeyValuePair(part);
                                if (kv != null && kv[0].startsWith("N:")) {
                                    keyValueStore.put(kv[0], kv[1]);
                                    newNodes.add(kv[0]);
                                }
                            }
                        }
                        nearestLatch.countDown();
                    }
                    @Override
                    public void onTimeout() {
                        nearestLatch.countDown();
                    }
                };

                pendingTransactions.put(nearestTxid, nearestCallback);
                sendPacket(nearestRequest, address, port);
                nearestLatch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
                nodesToQuery.addAll(newNodes);
            } else if (status[0] == 'N') {
                return null;
            }
        }
        return null;
    }

    private String[] parseKeyValuePair(String pair) {
        try {
            String[] parts = pair.trim().split(" ", 4);
            if (parts.length < 4) return null;
            int keySpaces = Integer.parseInt(parts[0]);
            String key = parts[1];
            int valueSpaces = Integer.parseInt(parts[2]);
            String value = parts[3];
            if (countSpaces(key) == keySpaces && countSpaces(value) == valueSpaces) {
                return new String[]{key, value};
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public boolean write(String key, String value) throws Exception {
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

            String txid = generateTransactionID();
            String writeRequest = txid + " W " + formatKeyValuePair(key, value);
            final boolean[] writeSuccess = {false};
            final CountDownLatch latch = new CountDownLatch(1);

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
                if (shouldStoreKey(keyHash)) keyValueStore.put(key, value);
            }
        }
        return success;
    }

    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
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

            String txid = generateTransactionID();
            String casRequest = txid + " C " + formatString(key) + formatString(currentValue) + formatString(newValue);
            final boolean[] casSuccess = {false};
            final CountDownLatch latch = new CountDownLatch(1);

            ResponseCallback callback = new ResponseCallback() {
                @Override
                public void onResponse(String response) {
                    if (response.length() >= 5 && response.charAt(3) == 'D') {
                        char result = response.charAt(5);
                        casSuccess[0] = (result == 'R' || result == 'A');
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
                if (shouldStoreKey(keyHash)) keyValueStore.put(key, newValue);
            }
        }
        return success;
    }
}