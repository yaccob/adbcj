package org.adbcj.h2.decoding;

import org.adbcj.h2.packets.SizeConstants;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public abstract class StatusReadingDecoder implements DecoderState {

    public final ResultAndState decode(DataInputStream stream, Channel channel) throws IOException {
        if(stream.available()< SizeConstants.INT_SIZE){
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
}
