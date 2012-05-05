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
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(invocationCount = 10, threadPoolSize = 5, timeOut = 30000)
public class ConnectTest {

	private ConnectionManager connectionManager;

	@Parameters({"url", "user", "password"})
	@BeforeTest
	public void createConnectionManager(String url, String user, String password) {
		connectionManager = ConnectionManagerProvider.createConnectionManager(url, user, password);
	}

	@AfterTest
	public void closeConnectionManager() {
		DbFuture<Void> closeFuture = connectionManager.close(true);
		closeFuture.getUninterruptably();
	}

	public void testConnectImmediateClose() throws Exception {
		final boolean[] callbacks = {false, false};
		final CountDownLatch latch = new CountDownLatch(2);

		DbFuture<Connection> connectFuture = connectionManager.connect().addListener(new DbListener<Connection>() {
			public void onCompletion(DbFuture<Connection> future) throws Exception {
				// Indicate that callback has been invoked
				callbacks[0] = true;
				latch.countDown();
			}
		});
		Connection connection = connectFuture.get(10, TimeUnit.SECONDS);
		assertTrue(!connection.isClosed());
		DbFuture<Void> closeFuture = connection.close().addListener(new DbListener<Void>() {
			public void onCompletion(DbFuture<Void> future) throws Exception {
				// Indicate that callback has been invoked
				callbacks[1] = true;
				latch.countDown();
			}
		});
		closeFuture.get(10, TimeUnit.SECONDS);
		assertTrue(connection.isClosed());
		latch.await(1, TimeUnit.SECONDS);
		assertTrue(callbacks[0], "Callback on connection future was not invoked");
		assertTrue(callbacks[1], "Callback on finalizeClose future was not invoked");
	}

	public void testConnectNonImmediateClose() throws DbException, InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);

		Connection connection = connectionManager.connect().get();
		assertTrue(!connection.isClosed());
		connection.close().addListener(new DbListener<Void>() {
			public void onCompletion(DbFuture<Void> future) throws Exception {
				// Indicate that finalizeClose callback has been invoked
				latch.countDown();
			}
		}).get();
		assertTrue(connection.isClosed());
        assertTrue(latch.await(1, TimeUnit.SECONDS));
	}


	public void testNonImmediateClose() throws Exception {
		Connection connection = connectionManager.connect().get();

		List<DbSessionFuture<ResultSet>> futures = new ArrayList<DbSessionFuture<ResultSet>>();

		for (int i = 0; i < 5; i++) {
			futures.add(connection.executeQuery(String.format("SELECT *, %d FROM simple_values", i)));
		}
		try {
			connection.close().get(10, TimeUnit.SECONDS);
		} catch (TimeoutException e) {
			for (DbSessionFuture<ResultSet> future : futures) {
				if (future.isDone()) {
					future.get(); // Will throw exception if failed
				} else {
					throw new AssertionError("future " + future + " did not complete in time");
				}
			}
			throw new AssertionError("finalizeClose future failed to complete");
		}
		assertTrue(connection.isClosed(), "Connection should be closed");
		for (DbSessionFuture<ResultSet> future : futures) {
			assertTrue(future.isDone(), "Request did not finish before connection was closed: " + future);
			assertFalse(future.isCancelled(), "Future was cancelled and should have been");
		}
	}

}
