import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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

public class Node implements NodeInterface {
    private String nodeName;
    private byte[] nodeHashID;
    private DatagramSocket socket;
    private int port;
    private Map<String, String> keyValueStore = new ConcurrentHashMap<>();
    private Map<Integer, List<AddressKeyValuePair>> addressKeyValuesByDistance = new ConcurrentHashMap<>();
    private Stack<String> relayStack = new Stack<>();
    private Map<String, ResponseCallback> pendingTransactions = new ConcurrentHashMap<>();
    private AtomicInteger txidCounter = new AtomicInteger(0);
    private ExecutorService executor = Executors.newCachedThreadPool();
    private Thread listenerThread;
    private static final int MAX_PACKET_SIZE = 1024;
    private static final int REQUEST_TIMEOUT = 3000;
    private static final int MAX_RETRIES = 3;

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

    private interface ResponseCallback {
        void onResponse(String response);
        void onTimeout();
    }

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

    private int calculateDistance(byte[] hash1, byte[] hash2) {
        if (hash1.length != hash2.length) {
            throw new IllegalArgumentException("Hashes must be of the same length");
        }
        int distance = 0;
        for (int i = 0; i < hash1.length; i++) {
            distance |= (hash1[i] ^ hash2[i]) & 0xFF;
        }
        return distance;
    }

    private void addAddressKeyValuePair(AddressKeyValuePair pair) {
        addressKeyValuesByDistance.computeIfAbsent(pair.distance, k -> new ArrayList<>()).add(pair);
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
                throw new Exception("Could not find an available port. Tried ports " +
                        portNumber + " through " + (portNumber + 5));
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
            try {
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
                            } catch (Exception e) {
                                // Silently handle errors
                            }
                        });
                    } catch (SocketTimeoutException e) {
                        // Ignore timeouts
                    } catch (IOException e) {
                        if (!socket.isClosed()) {
                            // Handle non-closed socket errors
                        }
                    }
                }
            } catch (Exception e) {
                // Silently handle errors
            }
        });
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    private void handleBootstrapMessage(String message, InetAddress sourceAddress, int sourcePort) {
        if (message.startsWith("*") && message.contains("W") && message.contains("N:")) {
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
                    String ip = addrParts[0];
                    int port = Integer.parseInt(addrParts[1]);
                    InetAddress addr = InetAddress.getByName(ip);
                    NodeAddress nodeAddr = new NodeAddress(key, addr, port);
                    addNodeAddress(key, nodeAddr);
                    keyValueStore.put(key, value);
                } catch (Exception e) {
                    // Silently handle errors
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
            // Silently handle errors
        }
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        if (socket == null) {
            throw new IllegalStateException("Socket not initialized. Call openPort first.");
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

    private void processMessage(DatagramPacket packet) {
        byte[] data = Arrays.copyOf(packet.getData(), packet.getLength());
        String message = new String(data, StandardCharsets.UTF_8);
        String[] parts = message.split("\\s+", 3);
        if (parts.length < 2) return;

        String txid = parts[0];
        char command = parts[1].charAt(0);

        ResponseCallback callback = pendingTransactions.remove(txid);
        if (callback != null) {
            callback.onResponse(message);
            return;
        }

        try {
            switch (command) {
                case 'G': handleNameRequest(txid, packet.getAddress(), packet.getPort()); break;
                case 'N': if (parts.length > 2) handleNearestRequest(txid, parts[2], packet.getAddress(), packet.getPort()); break;
                case 'E': if (parts.length > 2) handleKeyExistenceRequest(txid, parts[2], packet.getAddress(), packet.getPort()); break;
                case 'R': if (parts.length > 2) handleReadRequest(txid, parts[2], packet.getAddress(), packet.getPort()); break;
                case 'W': if (parts.length > 2) handleWriteRequest(txid, parts[2], packet.getAddress(), packet.getPort()); break;
                case 'C': if (parts.length > 2) handleCASRequest(txid, parts[2], packet.getAddress(), packet.getPort()); break;
                case 'V': if (parts.length > 2) handleRelayRequest(txid, parts[2], packet.getAddress(), packet.getPort()); break;
                case 'I': break; // Ignore info messages
            }
        } catch (Exception e) {
            // Silently handle errors
        }
    }

    private void handleNameRequest(String txid, InetAddress address, int port) throws Exception {
        String response = txid + " H " + formatString(nodeName);
        sendPacket(response, address, port);
    }

    private void handleNearestRequest(String txid, String hashIDHex, InetAddress address, int port) throws Exception {
        try {
            byte[] targetHashID = hexStringToByteArray(hashIDHex.trim());
            List<AddressKeyValuePair> nearestNodes = findNearestNodes(targetHashID, 3);
            StringBuilder response = new StringBuilder(txid + " O ");
            for (AddressKeyValuePair pair : nearestNodes) {
                response.append(formatKeyValuePair(pair.key, pair.value));
            }
            sendPacket(response.toString(), address, port);
        } catch (Exception e) {
            // Silently handle errors
        }
    }

    private void handleKeyExistenceRequest(String txid, String keyString, InetAddress address, int port) throws Exception {
        try {
            String key = parseString(keyString);
            if (key == null) return;
            char responseChar = keyValueStore.containsKey(key) ? 'Y' :
                    (shouldStoreKey(HashID.computeHashID(key)) ? 'N' : '?');
            String response = txid + " F " + responseChar;
            sendPacket(response, address, port);
        } catch (Exception e) {
            // Silently handle errors
        }
    }

    private void handleReadRequest(String txid, String keyString, InetAddress address, int port) throws Exception {
        try {
            String key = parseString(keyString);
            if (key == null) return;
            char responseChar;
            String value = "";
            if (keyValueStore.containsKey(key)) {
                responseChar = 'Y';
                value = keyValueStore.get(key);
            } else {
                responseChar = shouldStoreKey(HashID.computeHashID(key)) ? 'N' : '?';
            }
            String response = txid + " S " + responseChar + " " + formatString(value);
            sendPacket(response, address, port);
        } catch (Exception e) {
            // Silently handle errors
        }
    }

    private void handleWriteRequest(String txid, String message, InetAddress address, int port) throws Exception {
        try {
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
                responseChar = shouldStoreKey(HashID.computeHashID(key)) ? 'A' : 'X';
                if (responseChar == 'A') keyValueStore.put(key, value);
            }
            String response = txid + " X " + responseChar;
            sendPacket(response, address, port);
        } catch (Exception e) {
            // Silently handle errors
        }
    }

    private void handleCASRequest(String txid, String message, InetAddress address, int port) throws Exception {
        try {
            int firstSpace = message.indexOf(' ');
            if (firstSpace == -1) return;
            int keyCount = Integer.parseInt(message.substring(0, firstSpace));
            String rest = message.substring(firstSpace + 1);
            int keyEnd = -1;
            int spaceCount = 0;
            for (int i = 0; i < rest.length() && spaceCount <= keyCount; i++) {
                if (rest.charAt(i) == ' ') spaceCount++;
                if (spaceCount > keyCount) {
                    keyEnd = i;
                    break;
                }
            }
            if (keyEnd == -1) return;
            String key = rest.substring(0, keyEnd);
            String expectedAndNew = rest.substring(keyEnd + 1);
            firstSpace = expectedAndNew.indexOf(' ');
            if (firstSpace == -1) return;
            int expectedCount = Integer.parseInt(expectedAndNew.substring(0, firstSpace));
            rest = expectedAndNew.substring(firstSpace + 1);
            int expectedEnd = -1;
            spaceCount = 0;
            for (int i = 0; i < rest.length() && spaceCount <= expectedCount; i++) {
                if (rest.charAt(i) == ' ') spaceCount++;
                if (spaceCount > expectedCount) {
                    expectedEnd = i;
                    break;
                }
            }
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
                responseChar = shouldStoreKey(HashID.computeHashID(key)) ? 'A' : 'X';
                if (responseChar == 'A') keyValueStore.put(key, newValue);
            }
            String response = txid + " D " + responseChar;
            sendPacket(response, address, port);
        } catch (Exception e) {
            // Silently handle errors
        }
    }

    private void handleRelayRequest(String txid, String message, InetAddress address, int port) throws Exception {
        try {
            String destNodeName = parseString(message);
            if (destNodeName == null) return;
            int destNameEndPos = message.indexOf(' ', message.indexOf(' ') + 1) + 1;
            String innerMessage = message.substring(destNameEndPos);
            String innerTxid = innerMessage.substring(0, 4);
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
                        String relayResponse = txid + response.substring(4);
                        sendPacket(relayResponse, address, port);
                    } catch (Exception e) {
                        // Silently handle errors
                    }
                }
                @Override
                public void onTimeout() {}
            };
            pendingTransactions.put(innerTxid, callback);
            sendPacket(innerMessage, destAddress, destPort);
        } catch (Exception e) {
            // Silently handle errors
        }
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
                    // Skip this pair
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
            if (endIndex == -1) return content; // Adjusted to handle trailing content
            return content.substring(0, endIndex);
        } catch (Exception e) {
            return null;
        }
    }

    private String formatString(String s) {
        int spaceCount = countSpaces(s);
        return spaceCount + " " + s; // Removed trailing space
    }

    private String formatKeyValuePair(String key, String value) {
        return formatString(key) + " " + formatString(value);
    }

    private int countSpaces(String s) {
        int count = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == ' ') count++;
        }
        return count;
    }

    private byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    private String generateTransactionID() {
        int counter = txidCounter.getAndIncrement();
        return String.format("%04x", counter % 65536); // 4-character hex
    }

    private void sendPacket(String message, InetAddress address, int port) throws Exception {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
        socket.send(packet);
    }

    @Override
    public boolean isActive(String nodeName) throws Exception {
        if (!keyValueStore.containsKey(nodeName)) return false;
        String nodeValue = keyValueStore.get(nodeName);
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
                if (response.startsWith(txid + " H")) isActive[0] = true;
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
                    if (response.startsWith(txid + " F Y")) exists[0] = true;
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
        List<String> closestNodes = findClosestNodeNames(keyHash);

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

                ResponseCallback callback = new ResponseCallback() {
                    @Override
                    public void onResponse(String response) {
                        if (response.startsWith(txid + " S Y ")) {
                            value[0] = parseString(response.substring(txid.length() + 4));
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
                if (latch.await(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS) && value[0] != null) {
                    keyValueStore.put(key, value[0]);
                    return value[0];
                }
            }
        }
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
                    if (response.startsWith(txid + " X ")) {
                        char result = response.charAt(txid.length() + 2);
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
        if (keyValueStore.containsKey(key) && keyValueStore.get(key).equals(currentValue)) {
            keyValueStore.put(key, newValue);
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

            final boolean[] casSuccess = {false};
            final CountDownLatch latch = new CountDownLatch(1);
            String txid = generateTransactionID();
            String casRequest = txid + " C " + formatString(key) + " " + formatString(currentValue) + " " + formatString(newValue);

            ResponseCallback callback = new ResponseCallback() {
                @Override
                public void onResponse(String response) {
                    if (response.startsWith(txid + " D ")) {
                        char result = response.charAt(txid.length() + 2);
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
                keyValueStore.put(key, newValue);
                break;
            }
        }
        return success;
    }
}