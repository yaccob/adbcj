package org.adbcj.h2.server.responses;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public interface ServerToClientPacket {
    void writeToStream(DataOutputStream stream) throws IOException;
    int getLength();
}
