package org.adbcj.h2.packets;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class IoUtils {
    private IoUtils(){}


    /**
     * Write a string. The maximum string length is Integer.MAX_VALUE.
     *
     * @param theString the value
     * @return itself
     */
    public static void writeString(DataOutputStream out,String theString) throws IOException {
        if (theString == null) {
            out.writeInt(-1);
        } else {
            int len = theString.length();
            out.writeInt(len);
            for (int i = 0; i < len; i++) {
                out.writeChar(theString.charAt(i));
            }
        }
    }
    /**
     * Write a byte array.
     *
     * @param data the value
     * @return itself
     */
    public static void writeBytes(DataOutputStream out,byte[] data) throws IOException {
        if (data == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(data.length);
            out.write(data);
        }
    }
}
