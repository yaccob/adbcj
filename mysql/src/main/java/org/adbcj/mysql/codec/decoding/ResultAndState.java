package org.adbcj.mysql.codec.decoding;

import org.adbcj.mysql.codec.packets.ServerPacket;

public class ResultAndState{
    private final ServerPacket result;
    private final DecoderState newState;

    ResultAndState(DecoderState newState,ServerPacket result) {
        this.result = result;
        this.newState = newState;
    }

    public ServerPacket getResult() {
        return result;
    }

    public DecoderState getNewState() {
        return newState;
    }
}
