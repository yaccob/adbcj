package org.adbcj.h2.server.decoding;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class ResultAndState{
    private final DecoderState newState;
    private final boolean waitForMoreInput;

    private ResultAndState(DecoderState newState,boolean waitForMoreInput) {
        this.newState = newState;
        this.waitForMoreInput = waitForMoreInput;
    }


    public DecoderState getNewState() {
        return newState;
    }

    public boolean isWaitingForMoreInput() {
        return waitForMoreInput;
    }

    public static ResultAndState newState(DecoderState newState) {
        return new ResultAndState(newState, false);
    }
    public static ResultAndState waitForMoreInput(DecoderState newState) {
        return new ResultAndState(newState, true);
    }
}

