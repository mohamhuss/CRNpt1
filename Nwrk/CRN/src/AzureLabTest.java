// IN2011 Computer Networks
// Coursework 2024/2025
//
// This is a test program to show how Node.java can be used.
// It creates a single instance of Node.java.
// A bootstrapping stage gives the nodes the addresses of a few of the nodes on the Azure virtual lab
// Then it performs some basic tests on the network.
//
// Running this test is not enough to check that all of the features of your
// implementation work.  You will need to do your own testing as well.
//
// You will need to run this on the virtual lab computers.  If you run it on your own computer
// it will not be able to access the nodes on the virtual lab.
//
// You can use this to record the wireshark evidence of things working.
// But please be aware it does not test all of the features so you will need to modify it or
// write your own tests to show everything works.

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Random;

class AzureLabTest {
    public static void main (String [] args) {
        String emailAddress = "mohammed.hussain.29@city.ac.uk";
        if (false && emailAddress.indexOf('@') == -1) {
            System.err.println("Please set your e-mail address!");
            System.exit(1);
        }
        String ipAddress = "10.200.51.18";
        if (false && ipAddress.indexOf('.') == -1) {
            System.err.println("Please set your ip address!");
            System.exit(1);
        }

        try {
            // Create a node and initialise it
            Node node = new Node();
            String nodeName = "N:" + emailAddress;
            node.setNodeName(nodeName);

            int port = 20110;
            node.openPort(port);

            // Wait and hope that we get sent the address of some other nodes
            System.out.println("Waiting for another node to get in contact");
            node.handleIncomingMessages(12 * 1000);

            // Let's start with a test of reading key/value pairs stored on the network.
            // This should print out a poem.
            System.out.println("Getting the poem...");
            for (int i = 0; i < 7; ++i) {
                String key = "D:jabberwocky" + i;
                String value = node.read(key);
                if (value == null) {
                    System.err.println("Can't find poem verse " + i);
                    System.exit(2);
                } else {
                    System.out.println(value);
                }
            }

            // Now let's test writing a key/value pair
            System.out.println("Writing a marker so it's clear my code works");
            {
                String key = "D:" + emailAddress;
                String value = "It works!";
                boolean success = node.write(key, value);

                // Read it back to be sure
                System.out.println(node.read(key));
            }

            // Finally we will let other nodes know where we are
            // so that we can be contacted and can store data for others.
            System.out.println("Letting other nodes know where we are");
            node.write(nodeName, ipAddress + ":" + port);

            System.out.println("Handling incoming connections");
            node.handleIncomingMessages(0);


        } catch (Exception e) {
            System.err.println("Exception during AzureLabTest");
            e.printStackTrace(System.err);
            return;
        }
    }
}