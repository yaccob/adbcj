package org.adbcj.h2.server;

import org.adbcj.h2.server.responses.ServerToClientPacket;
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
public class H2TcpEncoder implements ChannelDownstreamHandler {
    private final static Logger logger = LoggerFactory.getLogger(H2TcpEncoder.class);

    @Override
    public void handleDownstream(ChannelHandlerContext context, ChannelEvent event) throws Exception {
        if (!(event instanceof MessageEvent)) {
            context.sendDownstream(event);
            return;
        }

        MessageEvent e = (MessageEvent) event;
        if (!(e.getMessage() instanceof ServerToClientPacket)) {
            context.sendDownstream(event);
            return;
        }

        ServerToClientPacket  response = (ServerToClientPacket) e.getMessage();
        ChannelBuffer buffer = ChannelBuffers.buffer(response.getLength());
        ChannelBufferOutputStream out = new ChannelBufferOutputStream(buffer);
        DataOutputStream dataOutputStream = new DataOutputStream(out);
        response.writeToStream(dataOutputStream);
        dataOutputStream.close();
        out.close();
        if(logger.isDebugEnabled()){
            logger.debug("Sent {} down stream",response);
        }
        Channels.write(context, e.getFuture(), buffer);
    }
}
