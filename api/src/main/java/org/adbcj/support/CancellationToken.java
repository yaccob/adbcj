package org.adbcj.support;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author roman.stoffel@gamlor.info
 */
public final class CancellationToken implements CancellationAction {
    public static final CancellationToken NO_CANCELLATION = new CancellationToken(CancelState.TOO_LATE_TO_CANCEL);

    private AtomicReference<CancelState> state;

    public CancellationToken() {
        this.state = new AtomicReference<CancelState>(CancelState.NOT_CANCELLED);
    }
    private CancellationToken(CancelState state) {
        this.state = new AtomicReference<CancelState>(state);
    }

    @Override
    public boolean cancel() {
        return state.compareAndSet(CancelState.NOT_CANCELLED, CancelState.CANCELLED);
    }

    /**
     * Tries to start this operation. If it can, it will return true.
     * If it returns false, the action is cancelled and should be aborted
     * @return true to continue, false to stop
     */
    public boolean tryStartOrIsCancel() {
        if(state.get()==CancelState.TOO_LATE_TO_CANCEL){
            return true;
        }
        return state.compareAndSet(CancelState.NOT_CANCELLED, CancelState.TOO_LATE_TO_CANCEL);
    }

    public boolean isCancelled() {
        return state.get() == CancelState.CANCELLED;
    }

    enum CancelState{
        NOT_CANCELLED,
        CANCELLED,
        TOO_LATE_TO_CANCEL
    }

}
