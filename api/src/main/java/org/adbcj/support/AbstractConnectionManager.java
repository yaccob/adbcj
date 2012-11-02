package org.adbcj.support;

import org.adbcj.CloseMode;
import org.adbcj.ConnectionManager;
import org.adbcj.DbFuture;
import org.adbcj.StandardProperties;

import java.util.Collections;
import java.util.Map;

/**
 * Abstract implementation of a {@link ConnectionManager}. It does following things for you:
 *
 *
 * @author roman.stoffel@gamlor.info
 */
public abstract class AbstractConnectionManager implements ConnectionManager {


    protected final Map<String, String> properties;

    public AbstractConnectionManager(Map<String, String> properties) {
        this.properties = Collections.unmodifiableMap(properties);
    }

    public DbFuture<Void> close(){
        return close(CloseMode.CLOSE_GRACEFULLY);
    }

    public int maxConnections(){
        try{
            int maxConnections = Integer.parseInt(properties.get(StandardProperties.MAX_QUEUE_LENGTH));
            if(maxConnections<=0){
                throw new IllegalArgumentException("The property "+StandardProperties.MAX_QUEUE_LENGTH+" has to be positive number");
            }
            return maxConnections;
        }catch (NumberFormatException e){
            throw new IllegalArgumentException("The property "+StandardProperties.MAX_QUEUE_LENGTH+" has to be positive number");
        }
    }
}
