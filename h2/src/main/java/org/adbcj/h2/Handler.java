package org.adbcj.h2;

import org.jboss.netty.channel.*;

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
