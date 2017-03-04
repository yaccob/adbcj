package org.adbcj.h2.decoding;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.adbcj.Connection;
import org.adbcj.DbCallback;
import org.adbcj.h2.H2Connection;

import java.io.DataInputStream;
import java.io.InputStream;
import java.util.List;


public class Decoder extends ByteToMessageDecoder {
    private static Object DecodedToken = new Object();
    private DecoderState currentState;
    private H2Connection connection;

    public Decoder(DbCallback<Connection> initialStateCompletion, H2Connection connection, StackTraceElement[] entry) {
        currentState = new FirstServerHandshake(initialStateCompletion, connection, entry);
        this.connection = connection;
    }

    public Decoder(DecoderState currentState, H2Connection connection, StackTraceElement[] entry) {
        this.currentState = currentState;
        this.connection = connection;
    }

    @Override
    public void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
        InputStream in = new ByteBufInputStream(buffer);
        in.mark(Integer.MAX_VALUE);
        try {
            final ResultAndState resultState = currentState.decode(new DataInputStream(in), ctx.channel());
            currentState = resultState.getNewState();
            if (resultState.isWaitingForMoreInput()) {
                in.reset();
            } else {
                out.add(DecodedToken);
            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            in.close();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        connection.tryCompleteClose(null);
    }
}
