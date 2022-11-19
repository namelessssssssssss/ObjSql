package com.objsql.handler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
//ChannelDuplexHandler是一个双向处理器。它也可以监听特定用户自定义事件发生时的操作
public class ClientNotActiveHandler extends ChannelDuplexHandler {

    //用于触发用户的特殊事件
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        IdleStateEvent stateEvent = (IdleStateEvent)evt;

        if(stateEvent.state() == IdleState.WRITER_IDLE){
            log.warn("在3600s内未接收到客户端{}读取请求",ctx.channel());
        }
        else if(stateEvent.state() == IdleState.READER_IDLE){
            log.warn("在7200s内未接收到客户端{}写入请求",ctx.channel());
        }

        super.userEventTriggered(ctx, evt);
    }
}
