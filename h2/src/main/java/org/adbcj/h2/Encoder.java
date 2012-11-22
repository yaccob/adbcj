package org.adbcj.h2;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

/**
 * @author roman.stoffel@gamlor.info
 */
public class Encoder extends FrameDecoder {
    @Override
    protected Object decode(ChannelHandlerContext ctx,
                            Channel channel,
                            ChannelBuffer buffer) throws Exception {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }
}
