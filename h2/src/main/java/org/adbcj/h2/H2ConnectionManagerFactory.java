package org.adbcj.h2;

import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerFactory;
import org.adbcj.DbException;
import org.adbcj.support.LoginCredentials;

import java.net.URI;
import java.util.Map;

/**
 * @author roman.stoffel@gamlor.info
 */
public class H2ConnectionManagerFactory implements ConnectionManagerFactory {
    private static final String PROTOCOL = "h2";
    private static final int DEFAULT_PORT = 8082;
    @Override
    public ConnectionManager createConnectionManager(String url,
                                                     String username,
                                                     String password,
                                                     Map<String, String> properties) throws DbException {
        try{
            URI uri = new URI(url);
            uri = new URI(uri.getSchemeSpecificPart());

            String host = uri.getHost();
            int port = uri.getPort();
            if (port < 0) {
                port = DEFAULT_PORT;
            }
            String path = uri.getPath().trim();
            if (path.length() == 0 || "/".equals(path)) {
                throw new DbException("You must specific a database in the URL path");
            }
            String schema = path.substring(1);

            return new H2ConnectionManager(uri.toString(),host, port,
                    new LoginCredentials(username.toUpperCase(),password, schema), properties);

        }catch (Exception e){
            throw DbException.wrap(e);
        }
    }

    @Override
    public boolean canHandle(String protocol) {
        return PROTOCOL.equalsIgnoreCase(protocol);
    }
}
