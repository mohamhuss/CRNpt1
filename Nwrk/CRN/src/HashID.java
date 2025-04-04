// IN2011 Computer Networks
// Coursework 2024/2025
//
// Construct the hashID for a string

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class HashID {

    public static byte [] computeHashID(String s) throws Exception {
	// What this does and how it works is covered in a later lecture
	MessageDigest md = MessageDigest.getInstance("SHA-256");
	md.update(s.getBytes(StandardCharsets.UTF_8));
	return md.digest();
    }
}
