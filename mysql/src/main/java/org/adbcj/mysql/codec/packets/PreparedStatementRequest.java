package org.adbcj.mysql.codec.packets;

import org.adbcj.mysql.codec.IoUtils;
import org.adbcj.mysql.codec.MysqlType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;


public class PreparedStatementRequest extends CommandRequest {
    private final int statementId;
    private final List<MysqlType> types;
    private final Object[] data;
    private byte[] writtenForm = null;

    public PreparedStatementRequest(int statementId, List<MysqlType> types, Object[] data) {
        super(Command.STATEMENT_EXECUTE);
        this.statementId = statementId;
        this.types = types;
        this.data = data;
    }

    @Override
    public boolean hasPayload() {
        return true;
    }

    /**
     * Protocol see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Execute_Packet_.28Tentative_Description.29
     */
    @Override
    protected void writePayLoad(OutputStream out) throws IOException {
        out.write(writtenBytes());
    }

    @Override
    public int getLength()  {
        return 1 + writtenBytes().length;
    }

    @Override
    public String toString() {
        return "PreparedStatementRequest{" +
                "statementId=" + statementId +
                '}';
    }

    byte[] writtenBytes() {
        if (writtenForm == null) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            try {
                IoUtils.writeInt(out, statementId);
                out.write((byte) 0); // flags: 0: CURSOR_TYPE_NO_CURSOR
                IoUtils.writeInt(out, 1); // reserved for future use. Currently always 1.
                out.write(IoUtils.nullMask(data));  //null_bit_map
                out.write(1); //  new_parameter_bound_flag
                for (MysqlType type : types) {
                    IoUtils.writeShort(out, type.getId());
                }
                writeParameters(out);
            } catch (IOException e) {
                throw new RuntimeException("Unexpected IO Exception: "+e.getMessage(),e);
            }
            writtenForm = out.toByteArray();
        }
        return writtenForm;
    }

    private void writeParameters(ByteArrayOutputStream out) throws IOException {
        if (types.size() != data.length) {
            throw new IllegalStateException("Expect type and data length to match. Type length: " +
                    types.size() + " data length " + data.length);
        }
        for (int i = 0; i < data.length;i++) {
            Object param = data[i];
            MysqlType type = types.get(i);
            if(null!=param){
                if (MysqlType.VARCHAR == type) {
                    IoUtils.writeLengthCodedString(out, param.toString(), StandardCharsets.UTF_8);
                } if (MysqlType.VAR_STRING == type) {
                    IoUtils.writeLengthCodedString(out, param.toString(), StandardCharsets.UTF_8);
                }  else {
                    throw new UnsupportedOperationException("Not yet implemented:" + type);
                }
            }
        }
    }
}
