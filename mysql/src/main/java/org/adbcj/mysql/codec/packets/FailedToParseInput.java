package org.adbcj.mysql.codec.packets;

public class FailedToParseInput extends ServerPacket{
    private final Exception exception;

    public FailedToParseInput(int packetLength, int packetNumber, Exception exception) {
        super(packetLength, packetNumber);
        this.exception = exception;
    }

    public Exception getException() {
        return exception;
    }
}
