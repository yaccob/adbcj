package org.adbcj.mysql.netty;

import org.adbcj.CloseMode;
import org.adbcj.Connection;
import org.adbcj.DbException;
import org.adbcj.DbFuture;
import org.adbcj.mysql.codec.*;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MysqlConnectionManager extends AbstractConnectionManager {

	private static final Logger logger = LoggerFactory.getLogger(MysqlConnectionManager.class);

	private static final String ENCODER = MysqlConnectionManager.class.getName() + ".encoder";
	private static final String DECODER = MysqlConnectionManager.class.getName() + ".decoder";
	private static final String MESSAGE_QUEUE = MysqlConnectionManager.class.getName() + ".queue";
    private final LoginCredentials credentials;

	private final ExecutorService executorService;
	private final ClientBootstrap bootstrap;
    private volatile DefaultDbFuture<Void> closeFuture = null;
    private final Set<AbstractMySqlConnection> connections = new HashSet<AbstractMySqlConnection>();
    private final AtomicInteger idCounter = new AtomicInteger();

	public MysqlConnectionManager(String host,
                                  int port,
                                  String username,
                                  String password,
                                  String schema,
                                  Map<String,String> properties) {
        super(properties);
        credentials = new LoginCredentials(username, password, schema);
		executorService = Executors.newCachedThreadPool();

		ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		bootstrap = new ClientBootstrap(factory);
		init(host, port);
	}


	private void init(String host, int port) {
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = Channels.pipeline();

                pipeline.addFirst(MESSAGE_QUEUE,new MessageQueuingHandler());
				pipeline.addLast(DECODER, new Decoder());
				pipeline.addLast(ENCODER, new Encoder());

				return pipeline;
			}
		});
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);
		bootstrap.setOption("remoteAddress", new InetSocketAddress(host, port));
	}

    public DbFuture<Void> close(CloseMode closeMode) throws DbException {
        ArrayList<AbstractMySqlConnection> connectionsCopy;
        synchronized (connections) {
            if (isClosed()) {
                return closeFuture;
            }
            closeFuture = new DefaultDbFuture<Void>();
            connectionsCopy = new ArrayList<AbstractMySqlConnection>(connections);
        }
        for (AbstractMySqlConnection connection : connectionsCopy) {
            connection.close(closeMode);
        }
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                bootstrap.releaseExternalResources();
                closeFuture.setResult(null);
            }
        });
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
                MysqlConnection connection = new MysqlConnection(MysqlConnectionManager.this, credentials, channel, connectFuture);
                channel.getPipeline().addLast("handler", new Handler(connection));

                final MessageQueuingHandler queuingHandler = channel.getPipeline().get(MessageQueuingHandler.class);
                //This is a terrible sinchronization hack
                // Currently needed because: We need the MessageQueuingHandler only as long as
                // The connection is not established. When it is, we need to remove it.
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (queuingHandler) {
                    queuingHandler.flush();
                    channel.getPipeline().remove(queuingHandler);
                }

                Decoder decoder = channel.getPipeline().get(Decoder.class);
                decoder.initializeWithSession(connection);


                if(future.getCause()!=null){
                    connectFuture.setException(future.getCause());
                }

            }
        });

        return connectFuture;
    }


    public boolean isClosed() {
        return closeFuture != null;
    }

    public int nextId() {
        return idCounter.incrementAndGet();
    }

    public void addConnection(AbstractMySqlConnection connection) {
        synchronized (connections) {
            connections.add(connection);
        }
    }

    public boolean removeConnection(AbstractMySqlConnection connection) {
        synchronized (connections) {
            return connections.remove(connection);
        }
    }

}

class Decoder extends FrameDecoder {

	private final MySqlClientDecoder decoder = new MySqlClientDecoder();

    private final AtomicReference<AbstractMySqlConnection> connection = new AtomicReference<AbstractMySqlConnection>(null);

	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
		 InputStream in = new ChannelBufferInputStream(buffer);
		 try {
			 return decoder.decode(connection.get(),in, false);
		 } finally {
			 in.close();
		 }
	}

    void initializeWithSession(AbstractMySqlConnection session){
        if(connection.getAndSet(session)!=null){
            throw new IllegalStateException("Expect that the initialisation is called only once");
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

class Handler extends SimpleChannelHandler {

	private final ProtocolHandler handler;

    public Handler(MysqlConnection connection) {
		this.handler =  new ProtocolHandler(connection);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		handler.messageReceived(e.getMessage());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
		Throwable t = handler.handleException(e.getCause());
		if (t != null) {
			// TODO: Pass exception on to connectionManager
			t.printStackTrace();
		}
	}

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		handler.connectionClosed();
	}

}