package org.adbcj.h2.decoding;

import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class ClosedConnectionState extends DecoderState {
    public static final ClosedConnectionState INSTANCE = new ClosedConnectionState();
    private ClosedConnectionState(){}

    @Override
    protected ResultAndState processFurther(DataInputStream stream, Channel channel, int status) throws IOException {
        throw new IllegalStateException("No further information expected");
    }

}
