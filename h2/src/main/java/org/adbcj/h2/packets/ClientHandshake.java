package org.adbcj.h2.packets;

import org.adbcj.DbException;
import org.adbcj.h2.decoding.Constants;
import org.adbcj.h2.decoding.IoUtils;
import org.adbcj.h2.h2.SHA256;

import java.io.DataOutputStream;
import java.io.IOException;

import static org.adbcj.h2.packets.SizeConstants.CHAR_SIZE;
import static org.adbcj.h2.packets.SizeConstants.INT_SIZE;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ClientHandshake implements ClientToServerPacket{

    private String database;
    private String originalUrl;
    private String userName;
    private byte[] pwdHash;

    public ClientHandshake(String database, String originalUrl, String userName, String password) {
        this.database = database;
        this.originalUrl = originalUrl;
        this.userName = userName;
        pwdHash = hashPassword(false, userName, password.toCharArray());
    }

    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {

        stream.writeInt(Constants.TCP_PROTOCOL_VERSION_12);
        stream.writeInt(Constants.TCP_PROTOCOL_VERSION_12);
        IoUtils.writeString(stream, database);
        IoUtils.writeString(stream, originalUrl);
        IoUtils.writeString(stream, userName);
        IoUtils.writeBytes(stream, pwdHash);
        writeFileHash(stream);

        writeKeys(stream);
    }

    @Override
    public int getLength() {
       return INT_SIZE + // Low version
               INT_SIZE +  // High version
               INT_SIZE + database.toCharArray().length* CHAR_SIZE +  // db-name
               INT_SIZE + originalUrl.toCharArray().length* CHAR_SIZE +  // url
               INT_SIZE + userName.toCharArray().length* CHAR_SIZE +  // url
               INT_SIZE + pwdHash.length +  // password
               INT_SIZE +  // empty file hash
               INT_SIZE +  // empty properties
               0;
    }

    private void writeFileHash(DataOutputStream stream) throws IOException {
        // File password hash, not in use
        stream.writeInt(-1);
    }

    private void writeKeys(DataOutputStream stream) throws IOException {
        // NO keys in use at the moment
        stream.writeInt(0);
    }


    private static byte[] hashPassword(boolean passwordHash, String userName, char[] password) {
        if (passwordHash) {
            return convertHexToBytes(new String(password));
        }
        if (userName.length() == 0 && password.length == 0) {
            return new byte[0];
        }
        return SHA256.getKeyPasswordHash(userName, password);
    }

    private static final int[] HEX_DECODE = new int['f' + 1];
    /**
     * Convert a hex encoded string to a byte array.
     *
     * @param s the hex encoded string
     * @return the byte array
     */
    public static byte[] convertHexToBytes(String s) {
        int len = s.length();
        if (len % 2 != 0) {
            throw new DbException("ERROR: 90003");
        }
        len /= 2;
        byte[] buff = new byte[len];
        int mask = 0;
        int[] hex = HEX_DECODE;
        try {
            for (int i = 0; i < len; i++) {
                int d = hex[s.charAt(i + i)] << 4 | hex[s.charAt(i + i + 1)];
                mask |= d;
                buff[i] = (byte) d;
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            throw DbException.wrap(e);
        }
        if ((mask & ~255) != 0) {
            throw new DbException("ERROR: 90004");
        }
        return buff;
    }
}


