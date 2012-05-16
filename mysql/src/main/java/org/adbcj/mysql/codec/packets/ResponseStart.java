package org.adbcj.mysql.codec.packets;

/**
 * When an error occurs in our driver, for example when the response callback throw an exception,
 * then we stop processing the rest of the response.
 *
 * But we need to know when the next response starts, in order to process the messages
 * of the next request. Therefore all messages which are the start of a response
 * implement this marker interface.
 * @author roman.stoffel@gamlor.info
 * @since 16.05.12
 */
public interface ResponseStart {
}
