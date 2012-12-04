package org.adbcj.h2.packets;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class UpdateExecute implements  ClientToServerPacket {
    public static final int COMMAND_EXECUTE_UPDATE = 3;
    private final int id;
    private final Object[] params;
    private static final Object[] NO_PARAMS = new Object[0];

    public UpdateExecute(int id) {
        this.id = id;
        this.params = NO_PARAMS;
    }
    public UpdateExecute(int id, Object[] params) {
        this.id = id;
        this.params = params;
    }

    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(COMMAND_EXECUTE_UPDATE);
        stream.writeInt(id);
        ParametersSerialisation.writeParams(stream, params);
    }

    @Override
    public int getLength() {
        return SizeConstants.INT_SIZE + // command execute update
                SizeConstants.INT_SIZE + // command id
                ParametersSerialisation.calculateParameterSize(params) +
                0;
    }
}
