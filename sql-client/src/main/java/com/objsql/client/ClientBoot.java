package com.objsql.client;

import com.objsql.client.codec.BaseClientCodec;
import com.objsql.client.handler.CommandHandler;
import com.objsql.client.handler.HeartBeatHandler;
import com.objsql.client.handler.ServerExceptionHandler;
import com.objsql.client.handler.ServerResponseHandler;

import com.objsql.common.protocol.codec.ProtocolFrameDecoder;
import com.objsql.common.util.ExceptionUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;


import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ClientBoot {
    public static final String SERVER_ADDR = "localhost";

    public static final int SERVER_PORT = 8080;

    public static void main(String[] args) {
        try {
            boot();
        }
        catch (InterruptedException e){
            log.error(ExceptionUtil.getStackTrace(e));
        }
    }


    private static void boot() throws InterruptedException {
        NioEventLoopGroup workers = new NioEventLoopGroup();
        //无状态处理器
        LoggingHandler loggingHandler = new LoggingHandler();
        BaseClientCodec baseClientCodec = new BaseClientCodec();
        ServerExceptionHandler exceptionHandler = new ServerExceptionHandler();
        IdleStateHandler idleStateHandler = new IdleStateHandler(10,20 , 30, TimeUnit.SECONDS);

        //获取队列中消息并发送的处理器
        CommandHandler commandHandler = new CommandHandler();

        new Bootstrap()
                .group(workers)
                //5秒未连接上服务器，认为连接超时
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,5000)
                .channel(NioSocketChannel.class)
                .handler(
                        new ChannelInitializer<NioSocketChannel>() {
                            @Override
                            protected void initChannel(NioSocketChannel nioSocketChannel) {
                                nioSocketChannel.pipeline()
                                        .addLast(new ProtocolFrameDecoder())
                                        .addLast(idleStateHandler)
                                        .addLast(new HeartBeatHandler())
                                        .addLast(exceptionHandler)
                                        .addLast(loggingHandler)
                                        .addLast(baseClientCodec)
                                        .addLast(new ServerResponseHandler())
                                        .addLast(commandHandler);
                            }
                        }
                ).connect(new InetSocketAddress(SERVER_ADDR, SERVER_PORT))
                .sync()
                .channel()
                .closeFuture()
                .addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) {
                        workers.shutdownGracefully();
                    }
                });


    }
}
