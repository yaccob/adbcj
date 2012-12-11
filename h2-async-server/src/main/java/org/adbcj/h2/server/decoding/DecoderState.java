package org.adbcj.h2.server.decoding;


import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public interface DecoderState {
    public ResultAndState decode(DataInputStream stream, Channel channel) throws IOException;
}
