/*
 *   Copyright (c) 2007 Mike Heath.  All rights reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package org.adbcj.tck.test;

import org.adbcj.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


@Test(invocationCount = 10, threadPoolSize = 5, timeOut = 50000)
public class SelectTest extends AbstractWithConnectionManagerTest {

    public void testSelectWhichReturnsNothing() throws Exception {
        Connection connection = connectionManager.connect().get();
        final CountDownLatch latch = new CountDownLatch(1);
        ResultSet resultSet = connection.executeQuery("SELECT int_val, str_val FROM simple_values where str_val LIKE 'Not-In-Database-Value'").addListener(new DbListener<ResultSet>() {
            public void onCompletion(DbFuture<ResultSet> future) {
                try {
                    Assert.assertNotNull(future.get());
                    latch.countDown();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
        }).get();
        Iterator<Row> i = resultSet.iterator();
        Assert.assertFalse(i.hasNext());

        connection.close();
    }

    public void testSimpleSelect() throws DbException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        Connection connection = connectionManager.connect().get();
        try {
            ResultSet resultSet = connection.executeQuery("SELECT int_val, str_val FROM simple_values ORDER BY int_val").addListener(new DbListener<ResultSet>() {
                public void onCompletion(DbFuture<ResultSet> future) {
                    try {
                        Assert.assertNotNull(future.get());
                        latch.countDown();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }
            }).get();

            Assert.assertEquals(6, resultSet.size());

            Iterator<Row> i = resultSet.iterator();

            Row nullRow = null;
            Row row = i.next();
            if (row.get(0).isNull()) {
                nullRow = row;
                row = i.next();
            }
            Assert.assertEquals(row.get(0).getInt(), 0);
            Assert.assertEquals(row.get(1).getValue(), "Zero");
            row = i.next();
            Assert.assertEquals(row.get(0).getInt(), 1);
            Assert.assertEquals(row.get(1).getValue(), "One");
            row = i.next();
            Assert.assertEquals(row.get(0).getInt(), 2);
            Assert.assertEquals(row.get(1).getValue(), "Two");
            row = i.next();
            Assert.assertEquals(row.get(0).getInt(), 3);
            Assert.assertEquals(row.get(1).getValue(), "Three");
            row = i.next();
            Assert.assertEquals(row.get(0).getInt(), 4);
            Assert.assertEquals(row.get(1).getValue(), "Four");

            if (i.hasNext() && nullRow == null) {
                nullRow = i.next();
            }

            Assert.assertEquals(nullRow.get(0).getValue(), null);
            Assert.assertEquals(nullRow.get(1).getValue(), null);


            Assert.assertTrue(!i.hasNext(), "There were too many rows in result set");

            Assert.assertTrue(latch.await(5, TimeUnit.SECONDS), "Expect callback call");
        } finally {
            connection.close().get();
        }
    }

    public void testSelectWithNullFields() throws DbException, InterruptedException {
        Connection connection = connectionManager.connect().get();

        ResultSet resultSet = connection.executeQuery("SELECT * FROM `table_with_some_values` WHERE `can_be_null_int` IS NULL").get();

        Assert.assertEquals(resultSet.get(0).get(1).getString(), null);
        Assert.assertEquals(resultSet.get(0).get(2).getString(), null);

        connection.close().get();

    }

    public void testMultipleSelectStatements() throws Exception {
        Connection connection = connectionManager.connect().get();

        List<DbFuture<ResultSet>> futures = new LinkedList<DbFuture<ResultSet>>();
        for (int i = 0; i < 50; i++) {
            futures.add(
                    connection.executeQuery(String.format("SELECT *, %d FROM simple_values", i))
            );
        }

        for (DbFuture<ResultSet> future : futures) {
            try {
                future.get(5, TimeUnit.MINUTES);
            } catch (TimeoutException e) {
                throw new AssertionError("Timed out waiting on future: " + future);
            }
        }
    }

    public void testWorksWithCallback() throws Exception {
        Connection connection = connectionManager.connect().get();


        DbSessionFuture<StringBuilder> resultFuture = connection.executeQuery("SELECT str_val FROM simple_values " +
                "WHERE str_val LIKE 'Zero'", buildStringInCallback(), new StringBuilder());

        StringBuilder result = resultFuture.get();
        Assert.assertEquals(result.toString(), expectedStringFromCallback());

        connection.close();

    }

    static String expectedStringFromCallback() {
        return "startFields-field(str_val)-endFields-startResults-startRow-value(Zero)-endRow-endResults";
    }

    static ResultHandler<StringBuilder> buildStringInCallback() {
        return new ResultHandler<StringBuilder>() {
            @Override
            public void startFields(StringBuilder accumulator) {
                accumulator.append("startFields-");
            }

            @Override
            public void field(Field field, StringBuilder accumulator) {
                accumulator.append("field(").append(field.getColumnLabel()).append(")-");
            }

            @Override
            public void endFields(StringBuilder accumulator) {
                accumulator.append("endFields-");
            }

            @Override
            public void startResults(StringBuilder accumulator) {
                accumulator.append("startResults-");
            }

            @Override
            public void startRow(StringBuilder accumulator) {
                accumulator.append("startRow-");
            }

            @Override
            public void value(Value value, StringBuilder accumulator) {
                accumulator.append("value(").append(value.getString()).append(")-");
            }

            @Override
            public void endRow(StringBuilder accumulator) {
                accumulator.append("endRow-");
            }

            @Override
            public void endResults(StringBuilder accumulator) {
                accumulator.append("endResults");
            }

            @Override
            public void exception(Throwable t, StringBuilder accumulator) {
            }
        };
    }

    public void testExceptionInCallbackHandler() throws Exception {
        Connection connection = connectionManager.connect().get();

        final CountDownLatch latch = new CountDownLatch(1);
        DbSessionFuture<StringBuilder> resultFuture = connection.executeQuery("SELECT str_val FROM simple_values " +
                "WHERE str_val LIKE 'Zero'", new AbstractResultHandler<StringBuilder>() {
            @Override
            public void startFields(StringBuilder accumulator) {
                throw new RuntimeException("Failure here");
            }

            @Override
            public void exception(Throwable t, StringBuilder accumulator) {
                latch.countDown();
            }
        }, new StringBuilder());
        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS), "Expect that exception method is called");
        try {
            resultFuture.get();
            Assert.fail("Expected exception to be propagated");
        } catch (DbException e) {
            Assert.assertTrue(e.getMessage().contains("Failure here"));

        }
    }
    public void testExceptionInCallbackHandlerDoesNotAffectNextCall() throws Exception {
        Connection connection = connectionManager.connect().get();

        DbSessionFuture<StringBuilder> errorCause = connection.executeQuery("SELECT str_val FROM simple_values " +
                "WHERE str_val LIKE 'Zero'", new AbstractResultHandler<StringBuilder>() {
            @Override
            public void startFields(StringBuilder accumulator) {
                throw new RuntimeException("Failure here");
            }
        }, new StringBuilder());
        ResultSet nextCall = connection.executeQuery("SELECT str_val FROM simple_values " +
                "WHERE str_val LIKE 'Two'").get();


        String result = nextCall.get(0).get(0).getString();
        Assert.assertEquals(result,"Two");
    }


    public void testBrokenSelect() throws Exception {
        Connection connection = connectionManager.connect().get();

        DbSessionFuture<ResultSet> future = connection.executeQuery("SELECT broken_query");
        try {
            future.get(5, TimeUnit.SECONDS);
            throw new AssertionError("Issues a bad query, future should have failed");
        } catch (DbException e) {
            // Pass
        } finally {
            connection.close().get();
        }
    }

}
