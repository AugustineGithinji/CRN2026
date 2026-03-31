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

// Complete this!
public class Node implements NodeInterface {

    private String nodeName;
    private DatagramSocket socket;
    private Map<String, String> localStore = new HashMap<>();

    public void setNodeName(String nodeName) throws Exception {
        this.nodeName = nodeName;
    }

    public void openPort(int portNumber) throws Exception {
        socket = new DatagramSocket(portNumber);
    }

    public void handleIncomingMessages(int delay) throws Exception {
        if (delay != 0) {
            socket.setSoTimeout(delay);
        } else {
            socket.setSoTimeout(0); // block forever
        }

        byte[] buffer = new byte[65536];
        while (true) {
            try {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                processMessage(message, packet.getAddress(), packet.getPort());
            } catch (java.net.SocketTimeoutException e) {
                return; // delay has elapsed, return normally
            }
        }
    }

    public boolean isActive(String nodeName) throws Exception {
        throw new Exception("Not implemented");
    }

    public void pushRelay(String nodeName) throws Exception {
        throw new Exception("Not implemented");
    }

    public void popRelay() throws Exception {
        throw new Exception("Not implemented");
    }

    public boolean exists(String key) throws Exception {
        throw new Exception("Not implemented");
    }

    public String read(String key) throws Exception {
        return localStore.get(key);
    }

    public boolean write(String key, String value) throws Exception {
        localStore.put(key, value);
        return true;
    }

    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        throw new Exception("Not implemented");
    }

    private void processMessage(String message, InetAddress senderAddress, int senderPort) throws Exception {
        // your message parsing logic here
    }

    private void sendMessage(String message, InetAddress address, int port) throws Exception {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
        socket.send(packet);
    }

}