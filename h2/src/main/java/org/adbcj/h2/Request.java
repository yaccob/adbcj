package org.adbcj.h2;

import org.adbcj.h2.decoding.DecoderState;
import org.adbcj.h2.packets.ClientToServerPacket;
import org.adbcj.support.DefaultDbFuture;

/**
 * @author roman.stoffel@gamlor.info
 */
public class Request {
    private final String description;
    private final DefaultDbFuture toComplete;
    private final DecoderState startState;
    private final ClientToServerPacket request;
    /**
     * A blocking request is one where the operation is split across multiple
     * request-responses, but are treated as one logical operation.
     * To avoid invalid interleaving, we need to ensure that now other request is sent
     * during the communication for such a request.
     */
    private final Request blocksFor;

    Request(String description,
            DefaultDbFuture toComplete,
            DecoderState startState,
            ClientToServerPacket request) {
        this.description = description;
        this.toComplete = toComplete;
        this.startState = startState;
        this.request = request;
        this.blocksFor = null;
    }
    Request(String description,
            DefaultDbFuture toComplete,
            DecoderState startState,
            ClientToServerPacket request, Request blocksFor) {
        this.description = description;
        this.toComplete = toComplete;
        this.startState = startState;
        this.request = request;
        this.blocksFor = blocksFor;
    }

    public DefaultDbFuture getToComplete() {
        return toComplete;
    }

    public DecoderState getStartState() {
        return startState;
    }

    public boolean isBlocking() {
        return blocksFor!=null;
    }

    public boolean unblockBy(Request nextRequest) {
        return blocksFor==nextRequest;
    }

    @Override
    public String toString() {
        return String.valueOf(description);
    }

    public ClientToServerPacket getRequest() {
        return request;
    }


}
