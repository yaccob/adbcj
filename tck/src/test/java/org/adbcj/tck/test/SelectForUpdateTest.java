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

import org.adbcj.Connection;
import org.adbcj.DbFuture;
import org.adbcj.DbListener;
import org.adbcj.ResultSet;
import org.adbcj.tck.TestUtils;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(timeOut = 5000)
public class SelectForUpdateTest extends AbstractWithConnectionManagerTest{

	public void testSelectForUpdate() throws Exception {
		final boolean[] invoked = {false, false};
		final AtomicBoolean locked = new AtomicBoolean(false);
		final AtomicBoolean error = new AtomicBoolean(false);
		final CountDownLatch latch1 = new CountDownLatch(1);
		final CountDownLatch latch2 = new CountDownLatch(1);
		
		Connection conn1 = connectionManager.connect().get();
		Connection conn2 = connectionManager.connect().get();
		
		// Get lock on locks table
		conn1.beginTransaction();
		TestUtils.selectForUpdate(conn1, new DbListener<ResultSet>() {
			public void onCompletion(DbFuture<ResultSet> future) {
				locked.set(true);
				invoked[0] = true;
				latch1.countDown();
			}
		}).get();
		
		// Try to get lock with second connection
		conn2.beginTransaction();
		DbFuture<ResultSet> future = TestUtils.selectForUpdate(conn2, new DbListener<ResultSet>() {
			public void onCompletion(DbFuture<ResultSet> future) {
				invoked[1] = true;
				if (!locked.get()) {
					error.set(true);
				}
				latch2.countDown();
			}
		});
		
		assertTrue(latch1.await(1, TimeUnit.SECONDS));
		assertTrue(invoked[0], "First SELECT FOR UPDATE callback should have been invoked");
		assertTrue(locked.get(), "locked should be set");
		assertFalse(invoked[1], "Second SELCT FOR UPDATE callback should not have been invoked yet");
		assertFalse(error.get());
		
		conn1.rollback().get();
		
		future.get();
		
		assertTrue(latch2.await(1, TimeUnit.SECONDS));
		assertTrue(invoked[1]);
		assertFalse(error.get(), "An error occurred during SELECT FOR UPDATE");
		conn2.rollback().get();
		
		// Close connections
		conn1.close().get();
		conn2.close().get();
	}
	
}
