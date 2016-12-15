package org.adbcj.support.stacktracing;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.adbcj.support.stacktracing.StackTracingOptions.FORCED_BY_INSTANCE;


public class StackTraceCapturingTest {
    @Test
    public void forcedCaptureOnCreation() {
        final Throwable execption = FORCED_BY_INSTANCE.captureStacktraceAtEntryPoint();
        Assert.assertNotNull(execption);
    }


}
