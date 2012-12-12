package org.adbcj.h2;

import org.adbcj.*;
import org.adbcj.h2.decoding.Decoder;
import org.adbcj.h2.packets.ClientHandshake;
import org.adbcj.support.AbstractConnectionManager;
import org.adbcj.support.CancellationAction;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.LoginCredentials;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author roman.stoffel@gamlor.info
 */
public class H2ConnectionManager extends AbstractConnectionManager {
    private final static Logger logger = LoggerFactory.getLogger(H2ConnectionManager.class);

    private final ExecutorService bossExecutor;
    private final ClientBootstrap bootstrap;
    private static final String ENCODER = H2ConnectionManager.class.getName() + ".encoder";
    private static final String DECODER = H2ConnectionManager.class.getName() + ".decoder";
    private final String url;
    private final LoginCredentials credentials;
    private final Map<String, String> keys;
    private final Set<H2Connection> connections = new HashSet<H2Connection>();

    public H2ConnectionManager(String url,String host,
                               int port,
                               LoginCredentials credentials,
                               Map<String, String> properties,
                               Map<String,String> keys) {
        super(properties);
        this.url = url;
        this.credentials = credentials;
        this.keys = keys;

        this.bossExecutor = Executors.newCachedThreadPool();
        ChannelFactory factory = new NioClientSocketChannelFactory(bossExecutor,
                Executors.newCachedThreadPool());
        bootstrap = new ClientBootstrap(factory);
        init(host, port);
    }


    private void init(String host, int port) {
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast(ENCODER, new Encoder());
                pipeline.addLast("handler", new Handler());

                return pipeline;
            }
        });
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("remoteAddress", new InetSocketAddress(host, port));
    }

    @Override
    public DbFuture<Connection> connect() {
        if (isClosed()) {
            throw new DbSessionClosedException("Connection manager closed");
        }
        logger.debug("Starting connection");

        final ChannelFuture channelFuture = bootstrap.connect();

        final DefaultDbFuture<Connection> connectFuture = new DefaultDbFuture<Connection>(new CancellationAction() {
            @Override
            public boolean cancel() {
                return channelFuture.cancel();
            }
        });

        channelFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                logger.debug("Connect completed");


                Channel channel = future.getChannel();
                H2Connection connection = new H2Connection(maxQueueLength(),H2ConnectionManager.this,channel);
                channel.getPipeline().addFirst(DECODER, new Decoder(connectFuture,connection));
                channel.write(
                        new ClientHandshake(credentials.getDatabase(),url,
                                credentials.getUserName(),
                                credentials.getPassword(),keys));


                if(future.getCause()!=null){
                    connectFuture.setException(future.getCause());
                }
                synchronized (connections){
                    connections.add(connection);
                }

            }
        });

        return connectFuture;
    }

    @Override
    public DbFuture<Void> doClose(CloseMode mode) throws DbException {
        ArrayList<H2Connection> connectionsCopy;
        synchronized (connections){
            connectionsCopy = new ArrayList<H2Connection>(connections);
        }
        final AtomicInteger toCloseCount = new AtomicInteger(connectionsCopy.size());
        final DefaultDbFuture closeFuture = new DefaultDbFuture<Void>();
        for (H2Connection connection : connectionsCopy) {
            connection.close(mode).addListener(new DbListener<Void>() {
                @Override
                public void onCompletion(DbFuture<Void> future) {
                    final int toCloseConnnectionCount = toCloseCount.decrementAndGet();
                    if (toCloseConnnectionCount <= 0) {
                        new Thread("Closing H2 ConnectionManager") {
                            @Override
                            public void run() {
                                bootstrap.releaseExternalResources();
                                closeFuture.setResult(null);
                            }
                        }.start();

                    }
                }
            });
        }
        return closeFuture;
    }
}
