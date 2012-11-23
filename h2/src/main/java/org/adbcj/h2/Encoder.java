package org.adbcj.h2;

import org.adbcj.h2.packets.ClientToServerPacket;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;

import java.io.DataOutputStream;

/**
 * @author roman.stoffel@gamlor.info
 */
class Encoder implements ChannelDownstreamHandler {

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
        Channels.write(context, e.getFuture(), buffer);
    }
}
