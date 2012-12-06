package org.adbcj.h2.decoding;

import org.adbcj.h2.H2DbException;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public interface DecoderState {
    /**
     * Decodes the stream according to it's state.
     *
     * Returns the new state and the parsed object.
     * If not all data is available yet, no object will be returned.
     * @param stream the data to read
     * @return state
     */
    public ResultAndState decode(DataInputStream stream, Channel channel) throws IOException;
    /**
     * Handle the exception occurred for this decoding staten
     *
     * Returns the new state
     * @return state
     */
    public ResultAndState handleException(H2DbException exception);



}

