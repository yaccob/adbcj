package org.adbcj.mysql.netty;

import org.adbcj.CloseMode;
import org.adbcj.Connection;
import org.adbcj.DbException;
import org.adbcj.DbFuture;
import org.adbcj.mysql.codec.ClientRequest;
import org.adbcj.mysql.codec.MySqlClientDecoder;
import org.adbcj.mysql.codec.MySqlClientEncoder;
import org.adbcj.mysql.codec.MySqlConnection;
import org.adbcj.mysql.codec.decoding.Connecting;
import org.adbcj.mysql.codec.decoding.DecoderState;
import org.adbcj.support.AbstractConnectionManager;
import org.adbcj.support.CancellationAction;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.LoginCredentials;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class MysqlConnectionManager extends AbstractConnectionManager {

	private static final Logger logger = LoggerFactory.getLogger(MysqlConnectionManager.class);

	private static final String ENCODER = MysqlConnectionManager.class.getName() + ".encoder";
	private static final String DECODER = MysqlConnectionManager.class.getName() + ".decoder";
	private static final String MESSAGE_QUEUE = MysqlConnectionManager.class.getName() + ".queue";
    private final LoginCredentials credentials;

	private final ClientBootstrap bootstrap;
    private final Set<MySqlConnection> connections = new HashSet<MySqlConnection>();
    private final AtomicInteger idCounter = new AtomicInteger();

    public MysqlConnectionManager(String host,
                                  int port,
                                  String username,
                                  String password,
                                  String schema,
                                  Map<String,String> properties) {
        super(properties);
        credentials = new LoginCredentials(username, password, schema);

        ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());
		bootstrap = new ClientBootstrap(factory);
		init(host, port);
	}


	private void init(String host, int port) {
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = Channels.pipeline();

				pipeline.addFirst(MESSAGE_QUEUE, new MessageQueuingHandler());
				pipeline.addLast(ENCODER, new Encoder());

				return pipeline;
			}
		});
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);
		bootstrap.setOption("remoteAddress", new InetSocketAddress(host, port));
	}

    protected DbFuture<Void> doClose(CloseMode closeMode) throws DbException {
        final DefaultDbFuture closeFuture;
        ArrayList<MySqlConnection> connectionsCopy;
        synchronized (connections) {
            closeFuture = new DefaultDbFuture<Void>();
            connectionsCopy = new ArrayList<MySqlConnection>(connections);
        }
        for (MySqlConnection connection : connectionsCopy) {
            connection.close(closeMode);
        }
        new Thread("Closing MySQL ConnectionManager"){
            @Override
            public void run() {
                bootstrap.releaseExternalResources();
                closeFuture.setResult(null);
            }
        }.start();
        return closeFuture;
    }

    public DbFuture<Connection> connect() {
        if (isClosed()) {
            throw new DbException("Connection manager closed");
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
                MySqlConnection connection = new MySqlConnection(maxQueueLength(), MysqlConnectionManager.this, channel);
                channel.getPipeline().addLast(DECODER, new Decoder(new Connecting(connectFuture, connection, credentials)));

                final MessageQueuingHandler queuingHandler = channel.getPipeline().get(MessageQueuingHandler.class);
                //This is a terrible sinchronization hack
                // Currently needed because: We need the MessageQueuingHandler only as long as
                // The connection is not established. When it is, we need to remove it.
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (queuingHandler) {
                    queuingHandler.flush();
                    channel.getPipeline().remove(queuingHandler);
                }

                if (future.getCause() != null) {
                    connectFuture.setException(future.getCause());
                }
                addConnection(connection);
            }
        });

        return connectFuture;
    }



    public int nextId() {
        return idCounter.incrementAndGet();
    }

    public void addConnection(MySqlConnection connection) {
        synchronized (connections) {
            connections.add(connection);
        }
    }

    public boolean removeConnection(MySqlConnection connection) {
        synchronized (connections) {
            return connections.remove(connection);
        }
    }

}

class Decoder extends FrameDecoder {

	private final MySqlClientDecoder decoder;

    public Decoder(DecoderState state) {
        decoder = new MySqlClientDecoder(state);
    }

    @Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
		 InputStream in = new ChannelBufferInputStream(buffer);
		 try {
			 return decoder.decode(in,channel, false);
		 } finally {
			 in.close();
		 }
	}

}

@ChannelHandler.Sharable
class Encoder implements ChannelDownstreamHandler {

	private final MySqlClientEncoder encoder = new MySqlClientEncoder();

	public void handleDownstream(ChannelHandlerContext context, ChannelEvent event) throws Exception {
        if (!(event instanceof MessageEvent)) {
            context.sendDownstream(event);
            return;
        }

        MessageEvent e = (MessageEvent) event;
        if (!(e.getMessage() instanceof ClientRequest)) {
            context.sendDownstream(event);
            return;
        }

        ClientRequest  request = (ClientRequest) e.getMessage();
        ChannelBuffer buffer = ChannelBuffers.buffer(4+request.getLength(MySqlClientEncoder.CHARSET));
        ChannelBufferOutputStream out = new ChannelBufferOutputStream(buffer);
    	encoder.encode((ClientRequest) e.getMessage(), out);
    	Channels.write(context, e.getFuture(), buffer);
	}
}

