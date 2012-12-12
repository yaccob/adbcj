package org.adbcj.h2.server;

import org.adbcj.h2.server.decoding.AcceptCommands;
import org.adbcj.h2.server.decoding.DecoderState;
import org.adbcj.h2.server.decoding.ResultAndState;
import org.adbcj.h2.server.responses.ErrorResponse;
import org.h2.message.DbException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.InputStream;

/**
 * @author roman.stoffel@gamlor.info
 */
public class H2TcpDecoder extends FrameDecoder {
    private DecoderState currentState;
    private AcceptCommands fallbackState;
    private static Logger logger = LoggerFactory.getLogger(H2TcpDecoder.class);

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
            if(currentState instanceof AcceptCommands){
                fallbackState = (AcceptCommands) currentState;
            }
            if(resultState.isWaitingForMoreInput()){
                in.reset();
                return null;
            }
            return "Parsed";
        } catch (DbException ex){
            logger.error("Error on server ",ex);
            channel.write(new ErrorResponse(ex));
            if(fallbackState!=null){
                currentState = fallbackState;
            }
            throw ex;
        }catch (Exception ex){
            logger.error("Error on server ", ex);
            throw ex;
        }finally {
            in.close();
        }
    }
}
