package org.adbcj.mysql.codec.decoding;

import org.adbcj.mysql.codec.*;
import org.adbcj.mysql.codec.packets.ServerGreeting;

import java.io.IOException;
import java.util.Set;

import static org.adbcj.mysql.codec.IoUtils.safeSkip;

/**
* @author roman.stoffel@gamlor.info
* @since 12.04.12
*/
class Connecting extends DecoderState {
    /**
     * The salt size in a server greeting
     */
    public static final int SALT_SIZE = 8;

    /**
     * The size of the second salt in a server greeting
     */
    public static final int SALT2_SIZE = 12;

    /**
     * Number of unused bytes in server greeting
     */
    public static final int GREETING_UNUSED_SIZE = 13;

    @Override
    public ResultAndState parse(int length,
                              int packetNumber,
                              BoundedInputStream in,
                              AbstractMySqlConnection connection) throws IOException {
        ServerGreeting serverGreeting = decodeServerGreeting(in, length, packetNumber);
        return result(RESPONSE,serverGreeting);
    }

    protected ServerGreeting decodeServerGreeting(BoundedInputStream in, int length, int packetNumber) throws IOException {
        int protocol = IoUtils.safeRead(in);
        String version = IoUtils.readNullTerminatedString(in, "ASCII");
        int threadId = IoUtils.readInt(in);

        byte[] salt = new byte[SALT_SIZE + SALT2_SIZE];
        in.read(salt, 0, SALT_SIZE);
        in.read(); // Throw away 0 byte

        Set<ClientCapabilities> serverCapabilities = IoUtils.readEnumSetShort(in, ClientCapabilities.class);
        MysqlCharacterSet charSet = MysqlCharacterSet.findById(in.read());
        Set<ServerStatus> serverStatus = IoUtils.readEnumSetShort(in, ServerStatus.class);
        safeSkip(in, GREETING_UNUSED_SIZE);

        in.read(salt, SALT_SIZE, SALT2_SIZE);
        // skip all plugin data for now
        in.read(new byte[in.getRemaining()-1]);
        in.read(); // Throw away 0 byte

        return new ServerGreeting(length, packetNumber, protocol, version, threadId, salt, serverCapabilities, charSet,
                serverStatus);
    }

    @Override
    public String toString() {
        return "CONNECTING";
    }
}
