package org.adbcj.h2.decoding;

import org.adbcj.Type;

import java.io.DataInputStream;
import java.io.IOException;


public final class ParameterInfo {
    private final Type type;
    private final long precision;
    private final int scale;
    private final int nullable;

    private ParameterInfo(Type type, long precision, int scale, int nullable) {
        this.type = type;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
    }

    public static ResultOrWait<ParameterInfo> read(DataInputStream input) throws IOException {
        final ResultOrWait<Integer> dataType = IoUtils.tryReadNextInt(input, ResultOrWait.Start);
        final ResultOrWait<Long> precision = IoUtils.tryReadNextLong(input, dataType);
        final ResultOrWait<Integer> scale = IoUtils.tryReadNextInt(input, precision);
        final ResultOrWait<Integer> nullable = IoUtils.tryReadNextInt(input, scale);
        if(nullable.couldReadResult){
            return ResultOrWait.result(new ParameterInfo(H2Types.typeCodeToType(dataType.result).getType(),precision.result,scale.result,nullable.result));
        } else{
            return ResultOrWait.WaitLonger;
        }
    }
}
