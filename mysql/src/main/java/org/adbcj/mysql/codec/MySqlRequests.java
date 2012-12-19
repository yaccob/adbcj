package org.adbcj.mysql.codec;

import org.adbcj.mysql.codec.packets.ServerGreeting;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.LoginCredentials;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class MySqlRequests {
    public static MySqlRequest createLoginRequest(ServerGreeting serverGreeting,
                                          DefaultDbFuture<MySqlConnection> connectFuture,
                                          MySqlConnection connection,
                                          LoginCredentials credentials) {
        return null;
    }
}
