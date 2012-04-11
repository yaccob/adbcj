package org.adbcj.mysql.codec.packets;

import java.io.UnsupportedEncodingException;

/**
 * @author roman.stoffel@gamlor.info
 * @since 11.04.12
 */
public class StringCommandRequest extends CommandRequest {
    private final String payload;

    public StringCommandRequest(Command command, String payload) {
        super(command);
        this.payload = payload;
    }


    @Override
    public int getLength(String charset) throws UnsupportedEncodingException {
        return 1 + payload.getBytes(charset).length;
    }

    public String getPayload() {
        return payload;
    }


}
