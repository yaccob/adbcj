package org.adbcj.support.stacktracing;

import junit.framework.Assert;
import org.testng.annotations.Test;

import static org.adbcj.support.stacktracing.StackTracingOptions.FORCED_BY_INSTANCE;

/**
 * @author roman.stoffel@gamlor.info
 */
public class StackTraceCapturingTest {
    @Test
    public void forcedCaptureOnCreation() {
        final Throwable execption = FORCED_BY_INSTANCE.captureStacktraceAtEntryPoint();
        Assert.assertNotNull(execption);
    }


}
