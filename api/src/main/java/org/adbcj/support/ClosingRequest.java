package org.adbcj.support;

/**
 * When we get the closed channel event we usually complete
 * all pending futures with an error. However a closing request is
 * completed instead, since it caused the channel to close in the first place.
 * @author roman.stoffel@gamlor.info
 * @since 22.03.12
 */
public interface ClosingRequest {
}
