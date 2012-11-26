package org.adbcj.h2.decoding;

import org.adbcj.h2.packets.SizeConstants;

import java.io.DataInputStream;
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

    public static boolean readBoolean(DataInputStream stream) throws IOException {
        return stream.readByte() == 1;
    }

    public static ResultOrWait<Boolean> tryReadNextBoolean(DataInputStream stream, ResultOrWait previousResult) throws IOException {
        if(!previousResult.couldReadResult){
            return previousResult;
        }
        if(stream.available()< SizeConstants.BYTE_SIZE){
            return ResultOrWait.WaitLonger;
        }
        return ResultOrWait.result(stream.readByte()==1);
    }

    public static ResultOrWait<Integer> tryReadNextInt(DataInputStream stream, ResultOrWait previousResult) throws IOException {
        if(!previousResult.couldReadResult){
            return previousResult;
        }
        if(stream.available()< SizeConstants.INT_SIZE){
            return ResultOrWait.WaitLonger;
        }
        return ResultOrWait.result(stream.readInt());
    }

    public static ResultOrWait<Long> tryReadNextLong(DataInputStream stream, ResultOrWait previousResult) throws IOException {
        if(!previousResult.couldReadResult){
            return previousResult;
        }
        if(stream.available()< SizeConstants.LONG_SIZE){
            return ResultOrWait.WaitLonger;
        }
        return ResultOrWait.result(stream.readLong());
    }

    public static ResultOrWait<String> tryReadNextString(DataInputStream stream, ResultOrWait previousResult) throws IOException {
        if(!previousResult.couldReadResult){
            return previousResult;
        }
        if(stream.available()< SizeConstants.INT_SIZE){
            return ResultOrWait.WaitLonger;
        }
        int stringLength = stream.readInt();
        if(stringLength<0){
            return ResultOrWait.result(null);
        }

        if(stream.available()< stringLength*SizeConstants.CHAR_SIZE){
            return ResultOrWait.WaitLonger;
        } else{
            char[] stringChars = new char[stringLength];
            for (int i = 0; i < stringLength; i++) {
                stringChars[i] = stream.readChar();
            }
            return ResultOrWait.result(new String(stringChars));
        }
    }
}
