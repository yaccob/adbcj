package org.adbcj.h2.server.responses;

import org.adbcj.h2.packets.SizeConstants;
import org.h2.expression.ParameterInterface;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @author roman.stoffel@gamlor.info
 */
public class PrepareResponse implements ServerToClientPacket{
    private final int stateToReturn;
    private final boolean readOnly;
    private final boolean query;
    private final ArrayList<? extends ParameterInterface> params;
    private final boolean writeParamters;

    public PrepareResponse(int stateToReturn,
                           boolean isReadOnly,
                           boolean isQuery,
                           ArrayList<? extends ParameterInterface> params,
                           boolean writeParamters) {

        this.stateToReturn = stateToReturn;
        readOnly = isReadOnly;
        query = isQuery;
        this.params = params;
        this.writeParamters = writeParamters;
    }

    @Override
    public void writeToStream(DataOutputStream stream) throws IOException {
        stream.writeInt(stateToReturn);
        stream.writeBoolean(readOnly);
        stream.writeBoolean(query);
        stream.writeInt(params.size());
        if(writeParamters){
            for (ParameterInterface param : params) {
                stream.writeInt(param.getType());
                stream.writeLong(param.getPrecision());
                stream.writeInt(param.getScale());
                stream.writeInt(param.getNullable());

            }
        }
    }

    @Override
    public int getLength() {
        return SizeConstants.INT_SIZE +// status code
               SizeConstants.BOOLEAN_SIZE + // read only
               SizeConstants.BOOLEAN_SIZE + // query
               SizeConstants.INT_SIZE + // params size
               paramsSize() + // params size
               0;

    }

    private int paramsSize() {
        if(writeParamters){
            return  (SizeConstants.INT_SIZE +
                        SizeConstants.LONG_SIZE +
                        SizeConstants.INT_SIZE +
                        SizeConstants.INT_SIZE) + params.size();
        }
        return 0;
    }
}
