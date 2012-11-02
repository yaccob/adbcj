package org.adbcj;

/**
 * Standart properties, which can be passed to {@link ConnectionManagerProvider#createConnectionManager}
 * via property map
 * @author roman.stoffel@gamlor.info
 */
public final class StandardProperties {
    private StandardProperties(){}

    /**
     * ADBCJ allows to have multiple async operations open running.
     * This parameter limits how many operations can be open before the implementation throws.
     * It is a per connection setting.
     *
     * This prevents the situation where a connection or system gets over whelmed with open requests.
     * By default this is 64.
     */
    public final static String MAX_QUEUE_LENGTH= "adbcj.maxQueueLength";
}
