package org.adbcj.h2.server;

import org.adbcj.h2.server.decoding.DecoderState;
import org.adbcj.h2.server.decoding.ResultAndState;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import java.io.DataInputStream;
import java.io.InputStream;

/**
 * @author roman.stoffel@gamlor.info
 */
public class H2TcpDecoder extends FrameDecoder {
    private DecoderState currentState;

    public H2TcpDecoder(DecoderState currentState) {
        this.currentState = currentState;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx,
                            Channel channel,
                            ChannelBuffer channelBuffer) throws Exception {
        InputStream in = new ChannelBufferInputStream(channelBuffer);
        in.mark(Integer.MAX_VALUE);
        try {
            final ResultAndState resultState = currentState.decode(new DataInputStream(in),ctx.getChannel());
            currentState = resultState.getNewState();
            if(resultState.isWaitingForMoreInput()){
                in.reset();
                return null;
            }
            return "Parsed";
        } catch (Exception ex){
            ex.printStackTrace();
            throw ex;
        }finally {
            in.close();
        }
    }
}
