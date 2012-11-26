package org.adbcj.h2.decoding;

import org.adbcj.Type;

/**
 * @author roman.stoffel@gamlor.info
 */

public final class H2Types {
    /**
     * The value type for STRING values.
     */
    public static final int STRING = 13;


    public static Type typeCodeToType(int typeCode) {
        switch (typeCode){
            case STRING:
                return Type.VARCHAR;
            default:
                return Type.fromJdbcType(typeCode);
        }
    }
}
