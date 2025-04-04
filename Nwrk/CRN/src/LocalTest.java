// IN2011 Computer Networks
// Coursework 2024/2025
//
// This is a test program to show how Node.java can be used.
// It creates a number of instances of Node.java, each in their own thread.
// A bootstrapping stage gives each of the nodes the addresses of a few others.
// Then it performs some basic tests on the network.
//
// Running this test is not enough to check that all of the features of your
// implementation work.  You will need to do your own testing as well.
// You are welcome to edit this but your submission must also work with the unedited version.
// You can run this on your own computer or on the virtual lab computers.
// Even when run on the virtual lab computers it will still only communicate between your nodes.
// So it is not suitable to record the wireshark evidence of things working.

import java.lang.Math;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Random;
import java.util.ArrayList;

class LocalTest {
    public static void main (String [] args) {
	try {
	    int numberOfNodes = 2;

	    // If you want to test with more nodes,
	    // set the number as a command line argument
	    if (args.length > 0) {
		int n = Integer.parseInt(args[0]);
		if (n >= 2 && n <= 10) {
		    numberOfNodes = n; 
		} else {
		    // If you want more than 10 nodes, you will need
		    // to change how bootstrapping is done
		}
	    }
	
	    // Create an array of nodes and initialise them
	    Node [] nodes = new Node [numberOfNodes];
	    for (int i = 0; i < numberOfNodes; ++i) {
		nodes[i] = new Node();
		nodes[i].setNodeName("N:test" + i);
		nodes[i].openPort(20110 + i);
	    }
	    
	    // Bootstrapping so that nodes know the addresses of some of the others
	    bootstrap(nodes);
	    
	    // Start each of the nodes running in a thread
	    // nodes[0] is handled by this program rather than a thread
	    for (int i = 1; i < numberOfNodes; ++i) {
		Integer j = i;
		Runnable r = () -> {
		    try {
			// These nodes just respond to messages
			nodes[j].handleIncomingMessages(0);
		    } catch (Exception e) {
			System.err.println("Unhandled exception in node " + j );
			e.printStackTrace(System.err);
		    }
		};
		Thread t = new Thread(r);
		t.start();
	    }

	    // Now we can test some of the functionality of node[0]

	    // We need some test text. This may be familiar.
	    ArrayList<String> lines = new ArrayList<String>();
	    lines.add("O Romeo, Romeo! wherefore art thou Romeo?");
	    lines.add("Deny thy father and refuse thy name;");
	    lines.add("Or, if thou wilt not, be but sworn my love,");
	    lines.add("And I'll no longer be a Capulet.");
	    lines.add("'Tis but thy name that is my enemy;");
	    lines.add("Thou art thyself, though not a Montague.");
	    lines.add("What's Montague? it is nor hand, nor foot,");
	    lines.add("Nor arm, nor face, nor any other part");
	    lines.add("Belonging to a man. O, be some other name!");
	    lines.add("What's in a name? that which we call a rose");
	    lines.add("By any other name would smell as sweet;");
	    lines.add("So Romeo would, were he not Romeo call'd,");
	    lines.add("Retain that dear perfection which he owes");
	    lines.add("Without that title. Romeo, doff thy name,");
	    lines.add("And for that name which is no part of thee");
	    lines.add("Take all myself.");

	    int successfulTests = 0;

	    // We will try to store them in the network.
	    // Which node the are stored on will depend on how many nodes there are in the network.
	    for (int i = 0; i < lines.size(); ++i) {
		String key = "D:Juliet-" + i;
		System.out.print("Trying to write " + key);
		boolean success = nodes[0].write(key, lines.get(i));
		if (success) {
		    System.out.println(" worked!");
		    ++successfulTests;
		} else {
		    System.out.println(" failed?");
		}
	    }
	    
	    // Read them back to make sure the other node handled them correctly
	    for (int i = 0; i < lines.size(); ++i) {
		String key = "D:Juliet-" + i;
		System.out.print("Trying to write " + key);
		String value = nodes[0].read(key);
		if (value == null) {
		    System.out.println(" not found?");
		} else if (value.equals(lines.get(i))) {
		    System.out.println(" worked!");
		    ++successfulTests;
		} else {
		    System.out.println(" unexpected string : " + value);
		}
	    }

	    if (successfulTests == 2*lines.size()) {
		System.out.println("All tests worked -- that's a good start!");
	    }
	    
	} catch (Exception e) {
	    System.err.println("Exception during localTest");
	    e.printStackTrace(System.err);
	    return;
	}
    }

    // This sends gives each node some initial address key/value pairs
    // You don't need to know how this works
    public static void bootstrap (Node [] nodes) throws Exception {
	int seed = 23; // Change for different initial network topologies
	Random r = new Random(seed);
	int n = nodes.length;
	double p =  Math.log( (double) n+5 ) / (double)n;

        DatagramSocket ds = new DatagramSocket(20099);
	byte[] contents = {0x30, 0x30, 0x20, 0x57, 0x20, 0x30, 0x20, 0x4E, 0x3A, 0x74, 0x65, 0x73, 0x74, 0x21, 0x20, 0x30, 0x20, 0x31, 0x32, 0x37, 0x2E, 0x30, 0x2E, 0x30, 0x2E, 0x31, 0x3A, 0x32, 0x30, 0x31, 0x31, 0x21, 0x20 };
	
	for (int i = 0; i < n; ++i) {
	    for (int j = 0; j < n; ++j) {
		if (i == j) {
		    // Skip
		} else {
		    if (r.nextDouble() <= p) {
			contents[0x00] = (byte)(0x41 + i);
			contents[0x01] = (byte)(0x42 + j);
			contents[0x0D] = (byte)(0x30 + j);
			contents[0x1F] = (byte)(0x30 + j);
			DatagramPacket packet = new DatagramPacket(contents, contents.length, InetAddress.getLocalHost(), 20110 + i);
			ds.send(packet);
		    }
		}
	    }
	}
	return;
    }
}
