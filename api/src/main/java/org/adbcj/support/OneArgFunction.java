package org.adbcj.support;

/**
 * @author roman.stoffel@gamlor.info
 */
public interface OneArgFunction<TArgument, TReturn> {
    TReturn apply(TArgument arg);
}
