package org.adbcj.mysql.netty;

import org.jboss.netty.channel.*;

import java.util.LinkedList;
import java.util.List;

class MessageQueuingHandler implements ChannelUpstreamHandler {

	private ChannelHandlerContext context;
	// Access must be synchronized on this
	private final List<MessageEvent> messageQueue = new LinkedList<MessageEvent>();

	// Access must be synchronized on this
	private boolean flushed = false;

	@Override
	public synchronized void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
			throws Exception {
		if (!flushed) {
			if (e instanceof MessageEvent) {
				if (context == null) {
					context = ctx;
				}
				messageQueue.add((MessageEvent) e);
			}
		}
	}

	public synchronized void flush() {
		for (MessageEvent event : messageQueue) {
			context.sendUpstream(event);
		}
		flushed = true;
	}

}
