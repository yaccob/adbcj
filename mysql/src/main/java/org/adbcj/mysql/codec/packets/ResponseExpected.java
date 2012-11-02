package org.adbcj.mysql.codec.packets;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ResponseExpected extends ServerPacket implements ResponseStart{
    private final ServerPacket msg;

    public ResponseExpected(ServerPacket msg) {
        super(msg.getPacketLength(),msg.getPacketNumber());
        this.msg = msg;
    }

    public ServerPacket realMessage() {
        return msg;
    }
}
