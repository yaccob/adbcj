package org.adbcj.h2.server;

import org.adbcj.h2.server.decoding.ServerHandshakeStart;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.DefaultChannelPipeline;

/**
 * @author roman.stoffel@gamlor.info
 */
public class H2TcpChannelFactory  implements ChannelPipelineFactory {
    @Override
    public ChannelPipeline getPipeline() throws Exception {

        final DefaultChannelPipeline pipeLine = new DefaultChannelPipeline();
        pipeLine.addFirst("decoder", new H2TcpDecoder(new ServerHandshakeStart()));
        pipeLine.addLast("encoder", new H2TcpEncoder());

        return pipeLine;
    }
}