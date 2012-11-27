package org.adbcj.h2;

import org.adbcj.h2.packets.ClientToServerPacket;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;

/**
 * @author roman.stoffel@gamlor.info
 */
class Encoder implements ChannelDownstreamHandler {
    private final static Logger logger = LoggerFactory.getLogger(Encoder.class);

    @Override
    public void handleDownstream(ChannelHandlerContext context, ChannelEvent event) throws Exception {
        if (!(event instanceof MessageEvent)) {
            context.sendDownstream(event);
            return;
        }

        MessageEvent e = (MessageEvent) event;
        if (!(e.getMessage() instanceof ClientToServerPacket)) {
            context.sendDownstream(event);
            return;
        }

        ClientToServerPacket  request = (ClientToServerPacket) e.getMessage();
        ChannelBuffer buffer = ChannelBuffers.buffer(request.getLength());
        ChannelBufferOutputStream out = new ChannelBufferOutputStream(buffer);
        DataOutputStream dataOutputStream = new DataOutputStream(out);
        request.writeToStream(dataOutputStream);
        dataOutputStream.close();
        out.close();
        if(logger.isDebugEnabled()){
            logger.debug("Sent {} down stream",request);
        }
        Channels.write(context, e.getFuture(), buffer);
    }
}
