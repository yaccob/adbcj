package org.adbcj.h2.decoding;

import org.adbcj.DbException;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public abstract class DecoderState {
    /**
     * Decodes the stream according to it's state.
     *
     * Returns the new state and the parsed object.
     * If not all data is available yet, no object will be returned.
     * @param stream the data to read
     * @return state
     */
    public final ResultAndState decode(DataInputStream stream, Channel channel) throws IOException {
        if(stream.available()<SizeConstants.INT_SIZE){
            return ResultAndState.waitForMoreInput(this);
        }
        final int status = stream.readInt();
        if(StatusCodes.STATUS_ERROR.isStatus(status)){
            // TODO
            System.out.println("TODOD");
        }
        return processFurther(stream, channel, status);
    }
    protected abstract ResultAndState processFurther(DataInputStream stream, Channel channel, int status) throws IOException;

    /**
     * Expect this status or throw
     * @param status
     * @param expectedStatus
     */
    protected void expectStatus(StatusCodes expectedStatus,int status) {
        if(!expectedStatus.isStatus(status)){
            throw new DbException("Expected status: "+status+" bus got: "+status);
        }
    }

}

