package org.adbcj.mysql.connect;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.adbcj.Connection;
import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerProvider;
import org.adbcj.StandardProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectTest {
	final static Logger log = LoggerFactory.getLogger(ConnectTest.class);

	public static void main(String[] args) throws InterruptedException {
		final int n = 10; // 10000 - test 'Too many connections' error handle
		final String url = "adbcj:mysql://localhost:3306/test";
		final String username = "test", password = "123456";
		final long tms = System.currentTimeMillis();
		final AtomicInteger success = new AtomicInteger(0);
		final CountDownLatch cnlat  = new CountDownLatch(n);
		final CountDownLatch colat  = new CountDownLatch(1);
		ConnectionManager cm = null;
		try {
			final Map<String, String> props = new HashMap<>();
			props.put(StandardProperties.CONNECTION_POOL_ENABLE, "false");
			cm = ConnectionManagerProvider
				.createConnectionManager(url, username, password, props);
			for(int i = 0; i < n; ++i) {
				CompletableFuture<Connection> cf = cm.connect();
				cf.whenComplete((c, e) -> {
					cnlat.countDown();
					if(e != null) {
						log.warn("<<< connect error", e);
					}
				}).thenCompose((c) -> {
					log.debug("connection is open? {}", c.isOpen());
					return c.close();
				}).thenRun(() -> {
					success.incrementAndGet();
					log.debug("<<< connection closed");
				});
				cm.connect((c, e) -> {
					cnlat.countDown();
					if(e == null) {
						// user callback exception
						throw new RuntimeException("error test");
					}
					log.warn("connct error: {}", e);
				});
			}
			cnlat.await();
		}finally {
			if(cm != null) {
				cm.close().whenComplete((r, e)->{
					if(e == null) {
						log.info("close completed");
					}else {
						log.warn("close error", e);
					}
					colat.countDown();
				});
			}
			colat.await();
			log.info("time: {}ms, ttl: {}, success: {}", 
				(System.currentTimeMillis()-tms), n, success.get());
		}
	}

}
