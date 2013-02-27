package org.adbcj.support.stacktracing;

/**
 * @author roman.stoffel@gamlor.info
 */
abstract class StackTraceCapturing {
    private static final StackTraceCapturing defaultCapturer = initializeCapturer();


    static MarkEntryPointToAdbcjException defaultCapture(){
        return defaultCapturer.capture();
    }

    protected abstract MarkEntryPointToAdbcjException capture();

    static class CaputureByDefault extends StackTraceCapturing{

        @Override
        protected MarkEntryPointToAdbcjException capture() {
            return new MarkEntryPointToAdbcjException();
        }
    }
    static class DoNotCapture extends StackTraceCapturing{

        @Override
        protected MarkEntryPointToAdbcjException capture() {
            return null;
        }
    }

    private static StackTraceCapturing initializeCapturer() {
        if(isPropertySet("org.adbcj.debug")){
            return new CaputureByDefault();
        } else{
            return new DoNotCapture();
        }
    }

    private static boolean isPropertySet(String name) {
        return Boolean.getBoolean(name);
    }
}

