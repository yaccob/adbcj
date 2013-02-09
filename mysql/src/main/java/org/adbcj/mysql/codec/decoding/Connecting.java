package org.adbcj.mysql.codec.decoding;

import org.adbcj.Connection;
import org.adbcj.mysql.codec.*;
import org.adbcj.mysql.codec.packets.LoginRequest;
import org.adbcj.mysql.codec.packets.ServerGreeting;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.LoginCredentials;
import io.netty.channel.Channel;

import java.io.IOException;
import java.util.Set;

import static org.adbcj.mysql.codec.IoUtils.safeSkip;

/**
* @author roman.stoffel@gamlor.info
* @since 12.04.12
*/
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

    private final DefaultDbFuture<Connection> connectFuture;
    private final MySqlConnection connection;
    private final LoginCredentials loginWith;

    public Connecting(DefaultDbFuture<Connection> connectFuture,
                      MySqlConnection connection, LoginCredentials loginWith) {
        this.connectFuture = connectFuture;
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
        channel.write(loginRequest);
        return result(new FinishLogin(connectFuture, connection),serverGreeting);
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
