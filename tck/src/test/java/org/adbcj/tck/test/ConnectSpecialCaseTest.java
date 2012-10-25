package org.adbcj.tck.test;

import org.adbcj.*;
import org.adbcj.tck.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 *
 */
public class ConnectSpecialCaseTest {

	private final Logger logger = LoggerFactory.getLogger(ConnectSpecialCaseTest.class);

	@Parameters({"url", "user", "password"})
	@Test(timeOut=60000)
	public void testConnectBadCredentials(String url, String user, String password) throws InterruptedException {
		final boolean[] callbacks = {false};
		final CountDownLatch latch = new CountDownLatch(1);

		ConnectionManager connectionManager = ConnectionManagerProvider.createConnectionManager(url, user, "__BADPASSWORD__");
		try {
			DbFuture<Connection> connectFuture = connectionManager.connect().addListener(new DbListener<Connection>() {
				public void onCompletion(DbFuture<Connection> future) throws Exception {
					callbacks[0] = true;
					latch.countDown();
				}
			});
			try {
				connectFuture.get();
				fail("Connect should have failed because of bad credentials");
			} catch (DbException e) {
				assertTrue(connectFuture.isDone(), "Connect future should be marked done even though it failed");
				assertTrue(!connectFuture.isCancelled(), "Connect future should not be marked as cancelled");
			}
			assertTrue(latch.await(1, TimeUnit.SECONDS), "Callback was not invoked in time");
			assertTrue(callbacks[0], "Connect future callback was not invoked with connect failure");
		} finally {
			connectionManager.close();
		}
	}

	@Parameters({"url", "user", "password"})
	@Test(timeOut=60000)
	public void testImmediateClose(String url, String user, String password) throws InterruptedException {
		ConnectionManager connectionManager = ConnectionManagerProvider.createConnectionManager(url, user, password);
		try {
			Connection lockingConnection = connectionManager.connect().get();
			Connection connection = connectionManager.connect().get();

			lockingConnection.beginTransaction();
			TestUtils.selectForUpdate(lockingConnection).get();

			List<DbSessionFuture<ResultSet>> futures = new ArrayList<DbSessionFuture<ResultSet>>();

			connection.beginTransaction();

			TestUtils.selectForUpdate(connection);
			for (int i = 0; i < 5; i++) {
				futures.add(connection.executeQuery(String.format("SELECT *, %d FROM simple_values", i)));
			}

			logger.debug("Closing connection");
			connection.close().get();
			logger.debug("Closed");

			logger.debug("Closing locking connection");
			lockingConnection.rollback().get();
			lockingConnection.close().get();
			logger.debug("Locking connection finalizeClose");

			assertTrue(connection.isClosed(), "Connection should be closed");
			for (DbSessionFuture<ResultSet> future : futures) {
				assertTrue(future.isCancelled(), "Future should have been cancelled at finalizeClose: " + future);
				assertTrue(future.isDone(), "Request did not finish before connection was closed: " + future);
			}
		} finally {
			connectionManager.close().get();
		}
	}

}
