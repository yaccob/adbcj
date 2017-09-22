package org.adbcj.mysql.codec.decoding;

import org.adbcj.Connection;
import org.adbcj.DbCallback;
import org.adbcj.mysql.MySqlConnection;
import org.adbcj.mysql.codec.*;
import org.adbcj.mysql.codec.packets.LoginRequest;
import org.adbcj.mysql.codec.packets.ServerGreeting;
import org.adbcj.support.LoginCredentials;
import io.netty.channel.Channel;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.adbcj.mysql.codec.IoUtils.safeSkip;


public class Connecting extends DecoderState {
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

    private final DbCallback<Connection> connected;
    private final StackTraceElement[] entry;
    private final MySqlConnection connection;
    private final LoginCredentials loginWith;

    public Connecting(DbCallback<Connection> connected,
                      StackTraceElement[] entry,
                      MySqlConnection connection,
                      LoginCredentials loginWith) {
        this.connected = connected;
        this.entry = entry;
        this.connection = connection;
        this.loginWith = loginWith;
    }

    @Override
    public ResultAndState parse(int length,
                                int packetNumber,
                                BoundedInputStream in,
                                Channel channel) throws IOException {
        ServerGreeting serverGreeting = decodeServerGreeting(in, length, packetNumber);
        LoginRequest loginRequest = new LoginRequest(loginWith,
                connection.getClientCapabilities(),
                connection.getExtendedClientCapabilities(),
                MysqlCharacterSet.UTF8_UNICODE_CI,serverGreeting.getSalt());
        channel.writeAndFlush(loginRequest);
        return result(new FinishLogin(connected, entry, connection),serverGreeting);
    }

    private ServerGreeting decodeServerGreeting(BoundedInputStream in, int length, int packetNumber) throws IOException {
        int protocol = IoUtils.safeRead(in);
        String version = IoUtils.readNullTerminatedString(in, StandardCharsets.US_ASCII);
        int threadId = IoUtils.readInt(in);

        byte[] salt = new byte[SALT_SIZE + SALT2_SIZE];
        in.readFully(salt, 0, SALT_SIZE);
        // Throw away 0 byte
        if(in.read()<0){
            throw new EOFException("Unexpected EOF. Expected to read 1 more byte");
        }

        Set<ClientCapabilities> serverCapabilities = IoUtils.readEnumSetShort(in, ClientCapabilities.class);
        MysqlCharacterSet charSet = MysqlCharacterSet.findById(in.read());
        Set<ServerStatus> serverStatus = IoUtils.readEnumSetShort(in, ServerStatus.class);
        safeSkip(in, GREETING_UNUSED_SIZE);

        in.readFully(salt, SALT_SIZE, SALT2_SIZE);
        // skip all plugin data for now
        in.readFully(new byte[in.getRemaining()-1]);
        if(in.read()<0){
            throw new EOFException("Unexpected EOF. Expected to read 1 more byte");
        }

        return new ServerGreeting(length,
                packetNumber,
                protocol,
                version,
                threadId,
                salt,
                serverCapabilities,
                charSet,
                serverStatus);
    }

    @Override
    public String toString() {
        return "CONNECTING";
    }
}
