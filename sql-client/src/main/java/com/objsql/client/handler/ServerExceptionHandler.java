package com.objsql.client.handler;

import com.objsql.common.util.common.ExceptionUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ChannelHandler.Sharable
public class ServerExceptionHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.warn("服务器端发生错误:{}", ExceptionUtil.getStackTrace(cause));
        super.exceptionCaught(ctx, cause);
    }
}
