package org.adbcj.h2.protocol;

/**
 * @author roman.stoffel@gamlor.info
 */
public enum CommandCodes {
    SESSION_SET_ID(12);

    private final int statusValue;

    private CommandCodes(int statusValue) {
        this.statusValue = statusValue;
    }

    public int getCommandValue() {
        return statusValue;
    }

    public boolean isCommand(int status) {
        return this.statusValue == status;
    }
}
