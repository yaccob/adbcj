package org.adbcj.h2.server;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * @author roman.stoffel@gamlor.info
 */
public class Server {
    private final int port;

    public Server(int port) {
        this.port = port;
    }

    public void start() {
        ServerBootstrap server = new ServerBootstrap(
                new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));



        server.setPipelineFactory(new H2TcpChannelFactory());
        server.setOption("backlog", 4096);
        server.setOption("child.tcpNoDelay", true);
        server.setOption("child.keepAlive", true);

        final Channel bind = server.bind(new InetSocketAddress("0.0.0.0", port));

    }
}
