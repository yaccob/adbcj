package org.adbcj.h2.packets;

import org.adbcj.support.CancellationToken;
import org.adbcj.h2.protocol.CommandCodes;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public class UpdateExecute extends ClientToServerPacket {
    private final int id;
    private final Object[] params;
    private static final Object[] NO_PARAMS = new Object[0];

    public UpdateExecute(int id, CancellationToken cancelSupport) {
        super(cancelSupport);
        this.id = id;
        this.params = NO_PARAMS;
    }
    public UpdateExecute(int id, CancellationToken cancelSupport, Object[] params) {
        super(cancelSupport);
        this.id = id;
        this.params = params;
    }

    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(CommandCodes.COMMAND_EXECUTE_UPDATE.getCommandValue());
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
