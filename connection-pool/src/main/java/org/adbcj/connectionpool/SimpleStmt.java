package org.adbcj.connectionpool;

import org.adbcj.PreparedStatement;

import java.util.LinkedHashMap;
import java.util.Map;

interface StmtCache {
    StmtItem get(String stmtString);
    StmtItem put(String stmtString,PreparedStatement stmt);


}
class NullCache implements StmtCache {
    public StmtItem get(String stmtString) {
        return null;
    }

    @Override
    public StmtItem put(String stmtString,final PreparedStatement stmt) {
        final StmtItem item = new StmtItem(stmt);
        return item;
    }


}

final class SimpleStmt implements StmtCache {
    private final Map<String,StmtItem> cache;

    public SimpleStmt(final int maxEntries) {
        cache = new LinkedHashMap<String,StmtItem>(maxEntries+1, 0.75F, true) {
            // This method is called just after a new entry has been added
            public boolean removeEldestEntry(Map.Entry<String,StmtItem> eldest) {
                if(size() > maxEntries){
                    eldest.getValue().close();
                    return true;
                } else{
                    return false;
                }
            }

        };
    }

    public synchronized StmtItem get(String stmtString){
        final StmtItem stmtItem = cache.get(stmtString);
        return stmtItem;
    }

    public synchronized StmtItem put(String stmtString,PreparedStatement stmt){
        StmtItem item = new StmtItem(stmt);
        item.aquireSharedAccess(); // the cached access.
        cache.put(stmtString, item);
        return item;
    }


}
