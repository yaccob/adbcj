package org.adbcj.mysql.codec.packets;

import org.adbcj.DbException;

import java.io.IOException;
import java.io.OutputStream;
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
    public int getLength(String charset) {
        return 1 + payloadAsBinary(charset).length;
    }

    @Override
    public boolean hasPayload() {
        return payload != null;
    }

    @Override
    protected void writePayLoad(OutputStream out, String charset) throws IOException {
        out.write(payloadAsBinary(charset));
    }


    private byte[] payloadAsBinary(String charset) {
        try {
            return payload.getBytes(charset);
        } catch (UnsupportedEncodingException e) {
            throw DbException.wrap(e);
        }
    }



}
