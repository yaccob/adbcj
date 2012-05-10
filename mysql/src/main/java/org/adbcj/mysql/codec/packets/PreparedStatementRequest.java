package org.adbcj.mysql.codec.packets;

import org.adbcj.mysql.codec.IoUtils;
import org.adbcj.mysql.codec.MysqlType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import static org.adbcj.support.UncheckedThrow.throwUnchecked;

/**
 * @author roman.stoffel@gamlor.info
 * @since 11.04.12
 */
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
    protected void writePayLoad(OutputStream out, String charset) throws IOException {
        out.write(writtenBytes());
    }

    @Override
    public int getLength(String charset) throws UnsupportedEncodingException {
        return 1 + writtenBytes().length;
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
                throw throwUnchecked(e);
            }
            writtenForm = out.toByteArray();
        }
        return writtenForm;
    }

    private void writeParameters(ByteArrayOutputStream out) throws IOException {
        for (Object param : data) {
            if (param instanceof String) {
                IoUtils.writeLengthCodedString(out, (String) param, "UTF-8");
            } else if(param instanceof Integer){
                IoUtils.writeLong(out, ((Integer) param).longValue(),4);
            } else if(param instanceof Long){
                IoUtils.writeLong(out, ((Long) param),8);
            } else {
                throw new UnsupportedOperationException("Not yet implemented:"+param.getClass());
            }
        }
    }
}
