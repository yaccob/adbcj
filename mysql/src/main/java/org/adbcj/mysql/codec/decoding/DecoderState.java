package org.adbcj.mysql.codec.decoding;

import org.adbcj.mysql.codec.BoundedInputStream;
import org.adbcj.mysql.codec.IoUtils;
import org.adbcj.mysql.codec.ServerStatus;
import org.adbcj.mysql.codec.packets.EofResponse;
import org.adbcj.mysql.codec.packets.ServerPacket;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;


public abstract class DecoderState {
    protected static final Logger logger = LoggerFactory.getLogger(DecoderState.class);
    protected static final String CHARSET = "UTF8";
    public static final int RESPONSE_EOF = 0xfe;



    public abstract ResultAndState parse(int length,
                                         int packetNumber,
                                         BoundedInputStream in, Channel channel) throws IOException;


    public ResultAndState result( DecoderState newState,ServerPacket result){
        return new ResultAndState(newState,result);
    }
    protected EofResponse decodeEofResponse(InputStream in, int length, int packetNumber, EofResponse.Type type) throws IOException {
        int warnings = IoUtils.readUnsignedShort(in);
        Set<ServerStatus> serverStatus = IoUtils.readEnumSetShort(in, ServerStatus.class);

        return new EofResponse(length, packetNumber, warnings, serverStatus, type);
    }
}

