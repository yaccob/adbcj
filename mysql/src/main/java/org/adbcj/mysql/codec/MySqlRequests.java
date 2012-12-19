package org.adbcj.mysql.codec;

import org.adbcj.mysql.codec.decoding.ExpectOK;
import org.adbcj.mysql.codec.packets.Command;
import org.adbcj.mysql.codec.packets.CommandRequest;
import org.adbcj.support.DefaultDbFuture;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class MySqlRequests {
    public static MySqlRequest createCloseRequest(MySqlConnection connection) {
        DefaultDbFuture<Void> future = new DefaultDbFuture<Void>();

        return new MySqlRequest("Close",future, new ExpectOK<Void>(future, connection),new CommandRequest(Command.QUIT));
    }
}
