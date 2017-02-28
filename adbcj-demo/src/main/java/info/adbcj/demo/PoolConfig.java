package info.adbcj.demo;

import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerProvider;

import java.util.HashMap;
import java.util.Map;


public class PoolConfig {
    public static void main(String[] args) {
        Map<String,String> config = new HashMap<String,String>();
        config.put("pool.maxConnections","50");
        config.put("pool.maxWaitForConnection","500"); // in milleseconds
        final ConnectionManager connectionManager = ConnectionManagerProvider.createConnectionManager(
                "adbcj:pooled:mysql://localhost/adbcj-demo",
                "adbcj",
                "adbc-pwd"
        );
    }
}
