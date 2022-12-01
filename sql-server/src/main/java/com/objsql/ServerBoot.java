package com.objsql;

import com.objsql.common.protocol.codec.ProtocolFrameDecoder;
import com.objsql.handler.ClientNotActiveHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;

import com.objsql.codec.BaseServerCodec;
import com.objsql.handler.ClientRequestHandler;

import java.util.concurrent.TimeUnit;

/**
 * 服务端
 */
@Slf4j
public class ServerBoot {


    public static void main(String[] args) {
        boot();
    }

    public static void boot(){

        NioEventLoopGroup bossGroup = new NioEventLoopGroup();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        ServerBootstrap boot = new ServerBootstrap();
        boot.channel(NioServerSocketChannel.class);
        boot.group(bossGroup, workerGroup);

        handlerRegister(boot);
        closeFutureRegister(boot.bind(8080));
    }

    static void handlerRegister(ServerBootstrap boot) {

        //无状态，可共用的处理器
        LoggingHandler loggingHandler = new LoggingHandler();
        BaseServerCodec baseServerCodec = new BaseServerCodec();
        ClientRequestHandler requestHandler = new ClientRequestHandler();

        boot.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ch.pipeline()
                        .addLast(new IdleStateHandler(3600, 7200,7200, TimeUnit.SECONDS))
                        .addLast(new ClientNotActiveHandler())
                        .addLast(new ProtocolFrameDecoder())
                    //    .addLast(loggingHandler)
                        .addLast(baseServerCodec)
                        .addLast(requestHandler);
            }
        });
    }

    static void closeFutureRegister(ChannelFuture future) {
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                  log.warn("客户端断开连接 : "+ future.channel());
            }
        });
    }
}
