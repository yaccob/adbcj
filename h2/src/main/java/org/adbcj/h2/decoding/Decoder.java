package org.adbcj.h2.decoding;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.adbcj.Connection;
import org.adbcj.h2.H2Connection;
import org.adbcj.support.DefaultDbFuture;

import java.io.DataInputStream;
import java.io.InputStream;

/**
 * @author roman.stoffel@gamlor.info
 */
public class Decoder extends ByteToMessageDecoder {
    private DecoderState currentState;
    private H2Connection connection;

    public Decoder(DefaultDbFuture<Connection> initialStateCompletion, H2Connection connection) {
        currentState = new FirstServerHandshake(initialStateCompletion,connection);
        this.connection = connection;
    }

    @Override
    public String decode(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
        InputStream in = new ByteBufInputStream(buffer);
        in.mark(Integer.MAX_VALUE);
        try {
            final ResultAndState resultState = currentState.decode(new DataInputStream(in),ctx.channel());
            currentState = resultState.getNewState();
            if(resultState.isWaitingForMoreInput()){
                in.reset();
                return null;
            }
            return null;
        } catch (Exception ex){
            ex.printStackTrace();
            throw ex;
        }finally {
            in.close();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        connection.tryCompleteClose();
    }
}
