package org.adbcj.mysql.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.*;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import org.adbcj.*;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class MysqlConnectionManager extends AbstractConnectionManager {

	private static final Logger logger = LoggerFactory.getLogger(MysqlConnectionManager.class);

	private static final String ENCODER = MysqlConnectionManager.class.getName() + ".encoder";
	private static final String DECODER = MysqlConnectionManager.class.getName() + ".decoder";
	private static final String MESSAGE_QUEUE = MysqlConnectionManager.class.getName() + ".queue";
    private final LoginCredentials credentials;

	private final Bootstrap bootstrap;
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

		bootstrap = new Bootstrap()
                .option(ChannelOption.TCP_NODELAY,true)
                .option(ChannelOption.SO_KEEPALIVE,true)
                .option(ChannelOption.SO_KEEPALIVE,true)
                .handler(new ChannelInitializer(){

                    @Override
                    public void initChannel(Channel ch) throws Exception {

                        ch.pipeline().addLast(ENCODER, new Encoder());

                    }
                });
	}



    protected DbFuture<Void> doClose(CloseMode closeMode) throws DbException {
        final DefaultDbFuture closeFuture;
        ArrayList<MySqlConnection> connectionsCopy;
        synchronized (connections) {
            closeFuture = new DefaultDbFuture<Void>();
            connectionsCopy = new ArrayList<MySqlConnection>(connections);
        }
        final AtomicInteger toCloseCount = new AtomicInteger(connectionsCopy.size());
        for (MySqlConnection connection : connectionsCopy) {
            connection.close(closeMode).addListener(new DbListener<Void>() {
                @Override
                public void onCompletion(DbFuture<Void> future) {
                    if(0==toCloseCount.decrementAndGet()){
                        new Thread("Closing MySQL ConnectionManager"){
                            @Override
                            public void run() {
                                bootstrap.shutdown();
                                closeFuture.setResult(null);
                            }
                        }.start();
                    }
                }
            });
        }
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


                Channel channel = future.channel();
                MySqlConnection connection = new MySqlConnection(maxQueueLength(), MysqlConnectionManager.this, channel);
                channel.pipeline().addLast(DECODER, new Decoder(new Connecting(connectFuture, connection, credentials)));
                channel.pipeline().addLast("end-handler", new Handler(connection));

//                final MessageQueuingHandler queuingHandler = channel.pipeline().get(MessageQueuingHandler.class);
//                //This is a terrible sinchronization hack
//                // Currently needed because: We need the MessageQueuingHandler only as long as
//                // The connection is not established. When it is, we need to remove it.
//                //noinspection SynchronizationOnLocalVariableOrMethodParameter
//                synchronized (queuingHandler) {
//                    queuingHandler.flush();
//                    channel.pipeline().remove(queuingHandler);
//                }

                if (future.cause() != null) {
                    connectFuture.setException(future.cause());
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

class Decoder extends ByteToMessageDecoder {

	private final MySqlClientDecoder decoder;

    public Decoder(DecoderState state) {
        decoder = new MySqlClientDecoder(state);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
        InputStream in = new ByteBufInputStream(buffer);
        try {
            return decoder.decode(in,ctx.channel(), false);
        } finally {
            in.close();
        }
    }
}


class Handler implements ChannelHandler {
    private final MySqlConnection connection;
    private static final Logger logger = LoggerFactory.getLogger(Handler.class);

    Handler(MySqlConnection connection) {
        this.connection = connection;
    }


    @Override
    public void beforeAdd(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void afterAdd(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void beforeRemove(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void afterRemove(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        throw new Error("Not implemented yet: TODO");  //TODO: Implement
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        logger.error("Unhandled exception",evt);
    }


}

@ChannelHandler.Sharable
class Encoder extends MessageToByteEncoder<ClientRequest> {
    private final static Logger log = LoggerFactory.getLogger(Encoder.class);

	private final MySqlClientEncoder encoder = new MySqlClientEncoder();

    @Override
    public void encode(ChannelHandlerContext ctx, ClientRequest msg, ByteBuf buffer) throws Exception {
        if(log.isDebugEnabled()){
            log.debug("Sending request: {}", msg);
        }

        ByteBufOutputStream out = new ByteBufOutputStream(buffer);
        encoder.encode(msg, out);
        out.flush();
    }
}

