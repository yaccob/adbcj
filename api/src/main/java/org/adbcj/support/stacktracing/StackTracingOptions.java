package org.adbcj.support.stacktracing;

public enum StackTracingOptions{
    /**
     * Only trace when the JVM "org.adbcj.debug" flag is set to true.
     *
     * So the user can activate it with -Dorg.adbcj.debug=true
     */
    GLOBAL_DEFAULT{
        @Override
        public MarkEntryPointToAdbcjException captureStacktraceAtEntryPoint() {
            return StackTraceCapturing.defaultCapture();
        }
    },
    /**
     * This {@link org.adbcj.ConnectionManagerFactory} or connection wants to have a stack-trace captured,
     * no mather what.
     */
    FORCED_BY_INSTANCE{
        @Override
        public MarkEntryPointToAdbcjException captureStacktraceAtEntryPoint() {
            return new MarkEntryPointToAdbcjException();
        }
    };

    public abstract MarkEntryPointToAdbcjException captureStacktraceAtEntryPoint();
}
