package org.adbcj.connectionpool;

import junit.framework.Assert;
import org.adbcj.DbFuture;
import org.adbcj.PreparedStatement;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author roman.stoffel@gamlor.info
 */
public class SimpleLRUTest {


    @Test
    public void cachesItems(){
        SimpleStmt toTest = new SimpleStmt(4);
        final PreparedStatement mockStmt =new MockPreparedQuery("stmt", new MockConnection(new MockConnectionManager()));
        toTest.put("stmt1", mockStmt);
        final StmtItem cached = toTest.get("stmt1");
        Assert.assertSame(mockStmt,cached.aquireSharedAccess());

    }
    @Test
    public void evictsAndClosesItems(){
        SimpleStmt toTest = new SimpleStmt(4);
        final AtomicBoolean wasClosed =new AtomicBoolean();
        final PreparedStatement mockStmt = new MockPreparedQuery("stmt", new MockConnection(new MockConnectionManager())){
                    @Override
                    public DbFuture<Void> close() {
                        wasClosed.set(true);
                        return super.close();
                    }
                };
        toTest.put("stmt1", mockStmt);
        toTest.put("stmt2", mockStmt);
        toTest.put("stmt3", mockStmt);
        toTest.put("stmt4", mockStmt);

        Assert.assertFalse(wasClosed.get());
        toTest.put("stmt5",new MockPreparedQuery("stmt", new MockConnection(new MockConnectionManager())));
        Assert.assertTrue(wasClosed.get());

    }
}
