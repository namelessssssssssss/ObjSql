package com.objsql.client.handler;

import com.objsql.client.message.ClientRequest;
import com.objsql.client.message.MissionQueue;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CommandHandler extends ChannelInboundHandlerAdapter {

    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(
            2, 3, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1024)
    );

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        executor.submit(() -> {
            while (true) {
                //检测到有新信息就写入
                ClientRequest request = MissionQueue.getFirst();
                executor.submit(() -> {
                    ctx.writeAndFlush(request);
                });
            }
        });
        super.channelActive(ctx);
    }


}
