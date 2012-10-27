package org.adbcj.tck.test;

import org.adbcj.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

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
				public void onCompletion(DbFuture<Connection> future) {
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


}
