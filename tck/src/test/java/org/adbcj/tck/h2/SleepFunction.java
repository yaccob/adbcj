package org.adbcj.tck.h2;

/**
 * @author roman.stoffel@gamlor.info
 */
public class SleepFunction {
    public static int sleep(int value) {
        long inSeconds = value*1000;
        try {
            Thread.sleep(inSeconds);
        } catch (InterruptedException e) {
            return 0;
        }
        return 0;
    }
}
