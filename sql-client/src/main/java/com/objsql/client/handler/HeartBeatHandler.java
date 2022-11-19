package com.objsql.client.handler;

import com.objsql.client.message.ClientRequest;
import com.objsql.client.message.MissionQueue;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HeartBeatHandler extends ChannelDuplexHandler {

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        log.debug("{}: ping...",ctx.channel());
        MissionQueue.submitAsync(new ClientRequest().ping().finish(), handledServerResponse ->{
            log.debug(handledServerResponse.getMessage());
        });

    }
}
