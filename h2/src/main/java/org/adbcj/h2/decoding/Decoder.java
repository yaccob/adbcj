package org.adbcj.h2.decoding;

import org.adbcj.Connection;
import org.adbcj.h2.H2Connection;
import org.adbcj.support.DefaultDbFuture;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import java.io.DataInputStream;
import java.io.InputStream;

/**
 * @author roman.stoffel@gamlor.info
 */
public class Decoder extends FrameDecoder {
    private DecoderState currentState;
    private H2Connection connection;

    public Decoder(DefaultDbFuture<Connection> initialStateCompletion, H2Connection connection) {
        currentState = new FirstServerHandshake(initialStateCompletion,connection);
        this.connection = connection;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        InputStream in = new ChannelBufferInputStream(buffer);
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


    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        super.channelClosed(ctx, e);
        connection.tryCompleteClose();
    }
}
