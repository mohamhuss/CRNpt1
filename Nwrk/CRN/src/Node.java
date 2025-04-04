import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

// Interface Definition
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

// Node Class Implementation
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
    private AtomicInteger txidCounter = new AtomicInteger(0); // For consistent transaction IDs

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
    }

    // Utility class for HashID (assumed provided)
    private static class HashID {
        static byte[] computeHashID(String input) throws Exception {
            // Placeholder: Replace with actual hash computation
            return input.getBytes(StandardCharsets.UTF_8);
        }
    }

    // Calculate distance between two hash IDs
    private int calculateDistance(byte[] hash1, byte[] hash2) {
        int minLength = Math.min(hash1.length, hash2.length);
        int distance = 0;
        for (int i = 0; i < minLength; i++) {
            distance += Math.abs((hash1[i] & 0xFF) - (hash2[i] & 0xFF));
        }
        distance += Math.abs(hash1.length - hash2.length) * 256;
        return distance;
    }

    // Add address key-value pair to routing table
    private void addAddressKeyValuePair(AddressKeyValuePair pair) {
        addressKeyValuesByDistance.computeIfAbsent(pair.distance, k -> new ArrayList<>()).add(pair);
    }

    @Override
    public void setNodeName(String nodeName) throws Exception {
        if (!nodeName.startsWith("N:")) {
            throw new IllegalArgumentException("Node name must start with 'N:'");
        }
        this.nodeName = nodeName;
        this.nodeHashID = HashID.computeHashID(nodeName);
        System.out.println("[DEBUG] Set node name: " + nodeName + ", HashID length: " + nodeHashID.length);
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
                } catch (BindException be) {
                    // Continue trying
                }
            }
            if (!success) {
                throw new Exception("[ERROR] Could not bind to any port from " + portNumber + " to " + (portNumber + 5));
            }
        }
        String localIP = InetAddress.getLocalHost().getHostAddress();
        String value = localIP + ":" + this.port;
        keyValueStore.put(nodeName, value);
        AddressKeyValuePair selfPair = new AddressKeyValuePair(nodeName, value);
        addAddressKeyValuePair(selfPair);
        System.out.println("[DEBUG] Opened port: " + this.port + ", Local IP: " + localIP + ", Stored value: " + value);
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
                    System.out.println("[DEBUG] Received message: '" + message + "' from " + packet.getAddress() + ":" + packet.getPort());
                    executor.submit(() -> {
                        try {
                            handleBootstrapMessage(message, packet.getAddress(), packet.getPort());
                            processMessage(packet);
                        } catch (Exception e) {
                            System.err.println("[ERROR] Error processing message: " + e.getMessage());
                        }
                    });
                }
            } catch (IOException e) {
                if (!socket.isClosed()) {
                    System.err.println("[ERROR] Socket error in listener: " + e.getMessage());
                }
            }
        });
        listenerThread.setDaemon(true);
        listenerThread.start();
        System.out.println("[DEBUG] Listener thread started on port: " + port);
    }

    private String generateTransactionID() {
        int counter = txidCounter.getAndIncrement();
        String txid = String.format("%04x", counter % 65536); // 4-character hex, e.g., "0001"
        System.out.println("[DEBUG] Generated TXID: " + txid);
        return txid;
    }

    private String formatString(String s) {
        int spaceCount = countSpaces(s);
        String formatted = spaceCount + " " + s + " ";
        System.out.println("[DEBUG] Formatted string: '" + formatted + "' for input: '" + s + "'");
        return formatted;
    }

    private int countSpaces(String s) {
        int count = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == ' ') count++;
        }
        return count;
    }

    private String parseString(String message) {
        try {
            int spaceIndex = message.indexOf(' ');
            if (spaceIndex == -1) return null;
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
            if (endIndex == -1) return null;
            String parsed = content.substring(0, endIndex);
            System.out.println("[DEBUG] Parsed string: '" + parsed + "' from message: '" + message + "'");
            return parsed;
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to parse string: '" + message + "': " + e.getMessage());
            return null;
        }
    }

    private void sendPacket(String message, InetAddress address, int port) throws Exception {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
        socket.send(packet);
        System.out.println("[DEBUG] Sent packet to " + address + ":" + port + ": '" + message + "'");
    }

    // Placeholder for bootstrap message handling
    private void handleBootstrapMessage(String message, InetAddress address, int port) throws Exception {
        // Simplified: Add node to keyValueStore if it's a bootstrap message
        if (message.startsWith("B ")) {
            String nodeInfo = parseString(message.substring(2));
            if (nodeInfo != null) {
                String[] parts = nodeInfo.split(" ");
                if (parts.length >= 2) {
                    String nodeName = parts[0];
                    String nodeValue = address.getHostAddress() + ":" + port;
                    keyValueStore.put(nodeName, nodeValue);
                    System.out.println("[DEBUG] Bootstrap added: " + nodeName + " -> " + nodeValue);
                }
            }
        }
    }

    private void processMessage(DatagramPacket packet) throws Exception {
        String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
        String[] parts = message.split(" ", 3);
        if (parts.length < 2) return;

        String txid = parts[0];
        String command = parts[1];

        if (command.equals("R") && parts.length == 3) {
            String key = parseString(parts[2]);
            if (key != null && keyValueStore.containsKey(key)) {
                String value = keyValueStore.get(key);
                String response = txid + " S Y " + formatString(value);
                sendPacket(response, packet.getAddress(), packet.getPort());
                System.out.println("[DEBUG] Processed read request for key: " + key + ", Response: " + response);
            }
        }
    }

    private List<String> findClosestNodeNames(byte[] keyHash) {
        List<String> closestNodes = new ArrayList<>();
        int minDistance = Integer.MAX_VALUE;
        for (Map.Entry<Integer, List<AddressKeyValuePair>> entry : addressKeyValuesByDistance.entrySet()) {
            if (entry.getKey() < minDistance) {
                minDistance = entry.getKey();
                closestNodes.clear();
                for (AddressKeyValuePair pair : entry.getValue()) {
                    closestNodes.add(pair.key);
                }
            }
        }
        System.out.println("[DEBUG] Closest nodes for hash: " + Arrays.toString(keyHash) + " -> " + closestNodes);
        return closestNodes;
    }

    @Override
    public String read(String key) throws Exception {
        if (keyValueStore.containsKey(key)) {
            System.out.println("[DEBUG] Key found locally: " + key + " -> " + keyValueStore.get(key));
            return keyValueStore.get(key);
        }

        byte[] keyHash = HashID.computeHashID(key);
        List<String> closestNodes = findClosestNodeNames(keyHash);

        for (String nodeName : closestNodes) {
            String nodeValue = keyValueStore.get(nodeName);
            if (nodeValue == null) continue;

            String[] parts = nodeValue.split(":");
            if (parts.length != 2) continue;

            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);

            for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
                final String[] value = { null };
                final CountDownLatch latch = new CountDownLatch(1);

                String txid = generateTransactionID();
                String formattedKey = formatString(key);
                String readRequest = txid + " R " + formattedKey;
                System.out.println("[DEBUG] Sending read request: '" + readRequest + "' to " + nodeName);

                ResponseCallback callback = new ResponseCallback() {
                    @Override
                    public void onResponse(String response) {
                        System.out.println("[DEBUG] Received response: '" + response + "'");
                        if (response.startsWith(txid + " S Y ")) {
                            String val = parseString(response.substring(txid.length() + 4));
                            if (val != null) value[0] = val;
                        }
                        latch.countDown();
                    }

                    @Override
                    public void onTimeout() {
                        System.out.println("[DEBUG] Timeout for request: '" + readRequest + "'");
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
                System.out.println("[DEBUG] Retry " + (attempt + 1) + " for node: " + nodeName);
            }
        }
        System.out.println("[DEBUG] Failed to retrieve key: " + key);
        return null;
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        Thread.sleep(delay); // Simulate processing delay
        System.out.println("[DEBUG] Handled incoming messages with delay: " + delay + "ms");
    }

    @Override
    public boolean isActive(String nodeName) throws Exception {
        boolean active = keyValueStore.containsKey(nodeName);
        System.out.println("[DEBUG] Checked if " + nodeName + " is active: " + active);
        return active;
    }

    @Override
    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
        System.out.println("[DEBUG] Pushed relay: " + nodeName + ", Stack: " + relayStack);
    }

    @Override
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            String popped = relayStack.pop();
            System.out.println("[DEBUG] Popped relay: " + popped + ", Stack: " + relayStack);
        } else {
            System.out.println("[DEBUG] Attempted to pop empty relay stack");
        }
    }

    @Override
    public boolean exists(String key) throws Exception {
        boolean exists = keyValueStore.containsKey(key);
        System.out.println("[DEBUG] Checked if key exists: " + key + " -> " + exists);
        return exists;
    }

    @Override
    public boolean write(String key, String value) throws Exception {
        keyValueStore.put(key, value);
        System.out.println("[DEBUG] Wrote key: " + key + " -> " + value);
        return true; // Placeholder: Assume success
    }

    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        String existing = keyValueStore.get(key);
        if (existing != null && existing.equals(currentValue)) {
            keyValueStore.put(key, newValue);
            System.out.println("[DEBUG] CAS succeeded for key: " + key + ", Updated from '" + currentValue + "' to '" + newValue + "'");
            return true;
        }
        System.out.println("[DEBUG] CAS failed for key: " + key + ", Expected '" + currentValue + "', Found '" + existing + "'");
        return false;
    }
}