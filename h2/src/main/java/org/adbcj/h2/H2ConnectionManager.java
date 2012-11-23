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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    public H2ConnectionManager(String url,String host,
                               int port,
                               LoginCredentials credentials,
                               Map<String, String> properties) {
        super(properties);
        this.url = url;
        this.credentials = credentials;

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
                channel.write(
                        new ClientHandshake(credentials.getDatabase(),url,
                                credentials.getUserName(),
                                credentials.getPassword()));
                H2Connection connection = new H2Connection();
                channel.getPipeline().addLast(DECODER, new Decoder(connectFuture,connection));
                channel.getPipeline().addLast("handler", new Handler(connection));

                if(future.getCause()!=null){
                    connectFuture.setException(future.getCause());
                }

            }
        });

        return connectFuture;
    }

    @Override
    protected DbFuture<Void> doClose(CloseMode mode) {
        return new DefaultDbFuture<Void>();
    }
}
