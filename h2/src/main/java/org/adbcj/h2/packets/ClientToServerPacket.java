package org.adbcj.h2.packets;

import org.adbcj.h2.CancellationToken;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 */
public abstract class ClientToServerPacket {
    private final CancellationToken cancelSupport;

    protected ClientToServerPacket(CancellationToken cancelSupport) {
        this.cancelSupport = cancelSupport;
    }

    public abstract void writeToStream(DataOutputStream stream) throws IOException;

    public abstract int getLength();

    public boolean startWriteOrCancel(){
        return cancelSupport.tryStartOrIsCancel();
    }
}
