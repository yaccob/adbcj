package org.adbcj.h2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.adbcj.h2.packets.ClientToServerPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;

/**
 * @author roman.stoffel@gamlor.info
 */
class Encoder extends MessageToByteEncoder<ClientToServerPacket> {
    private final static Logger logger = LoggerFactory.getLogger(Encoder.class);


    @Override
    public void encode(ChannelHandlerContext ctx, ClientToServerPacket request, ByteBuf buffer) throws Exception {

        if(request.startWriteOrCancel()){
            ByteBufOutputStream out = new ByteBufOutputStream(buffer);
            DataOutputStream dataOutputStream = new DataOutputStream(out);
            request.writeToStream(dataOutputStream);
            dataOutputStream.close();
            out.close();
            if(logger.isDebugEnabled()){
                logger.debug("Sent {} to server",request);
            }
        } else{
            // was cancelled
            logger.debug("Message has been cancelled {}",request);
        }
    }
}
