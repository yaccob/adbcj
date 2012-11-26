package org.adbcj.h2;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * @author roman.stoffel@gamlor.info
 */
class Handler  extends SimpleChannelHandler {
    public Handler() {
        //To change body of created methods use File | Settings | File Templates.
    }


    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        //System.out.println("Msg");
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        super.exceptionCaught(ctx, e);
        e.getCause().printStackTrace();
    }


}
