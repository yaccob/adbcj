package org.adbcj.h2.decoding;

/**
 * @author roman.stoffel@gamlor.info
 */
public interface ReadContinuation<T> {
    ResultAndState continueWith(T readResult);
}
