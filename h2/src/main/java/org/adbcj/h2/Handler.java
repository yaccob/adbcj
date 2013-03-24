package org.adbcj.h2;

import io.netty.channel.*;

/**
 * @author roman.stoffel@gamlor.info
 */
class Handler  implements ChannelHandler {
    public Handler() {
        //To change body of created methods use File | Settings | File Templates.
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
        cause.getCause().printStackTrace();
    }


}
