package org.adbcj.h2.packets;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class SizeConstants {
    public static final int CHAR_SIZE = 2;
    public static final int INT_SIZE = 4;
    public static final int BYTE_SIZE = 1;
    public static final int LONG_SIZE = 8;
    public static final int DOUBLE_SIZE = 8;

    private SizeConstants(){}

    static int lengthOfString(String param) {
        return CHAR_SIZE * ((String) param).toCharArray().length;
    }
}
