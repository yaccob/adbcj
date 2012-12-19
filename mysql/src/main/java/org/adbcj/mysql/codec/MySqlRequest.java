package org.adbcj.mysql.codec;

import org.adbcj.mysql.codec.decoding.DecoderState;
import org.adbcj.support.DefaultDbFuture;

/**
 * @author roman.stoffel@gamlor.info
 */
public class MySqlRequest {
    private final String description;
    private final DefaultDbFuture toComplete;
    private final DecoderState startState;
    private final ClientRequest request;

    MySqlRequest(String description,
                 DefaultDbFuture toComplete,
                 DecoderState startState,
                 ClientRequest request) {
        this.description = description;
        this.toComplete = toComplete;
        this.startState = startState;
        this.request = request;
    }

    @Override
    public String toString() {
        return "MySqlRequest{"+ description +  '}';
    }

    public ClientRequest getRequest() {
        return request;
    }

    public DecoderState getDecoderState() {
        return startState;
    }

    public DefaultDbFuture getFuture() {
        return toComplete;
    }
}
