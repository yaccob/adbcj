package org.adbcj.h2.packets;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public interface ClientToServerPacket {


    void writeToStream(DataOutputStream stream) throws IOException;

    int getLength();
}
