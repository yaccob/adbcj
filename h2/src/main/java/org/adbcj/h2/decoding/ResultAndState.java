package org.adbcj.h2.decoding;

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
    public static ResultAndState switchState(DecoderState newState) {
        return new ResultAndState(newState, true);
    }

    public static ResultAndState waitForMoreInput(DecoderState newState) {
        return new ResultAndState(newState, true);
    }
}
