import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultPromise;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.*;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;

@Slf4j
public class NettyTest {


    @Test
    void server() throws InterruptedException {

        EventLoopGroup group = new DefaultEventLoopGroup();
        NioEventLoopGroup nioGroup = new NioEventLoopGroup();

        //1,启动器，组装并启动服务器
        new ServerBootstrap()
                //2，添加组件：Boss/WorkerEventLoop(selector,thread)
                // selector + thread可以看作一个EventLoop，一个监听并处理channel事件的单元。group可以有多个EventLoop
                // 此处的LoopGroup为worker
                .group(nioGroup)
                //3，选择服务器的ServerSocket的实现。支持多个channel的实现，OIO，BIO等，此处使用NIO实现。
                .channel(NioServerSocketChannel.class)
                //4，定义worker处理事件时的逻辑。如worker（child）负责读写，boss负责处理连接。Netty默认封装好了boss。
                .childHandler(
                        //5，初始化channel
                        new ChannelInitializer<NioSocketChannel>() {
                            @Override
                            protected void initChannel(NioSocketChannel channel) {

                                channel.pipeline().addLast(new LoggingHandler(LogLevel.DEBUG));
                                //6，在处理管线中添加数据编解码器。管线中的原始数据是ByteBuf，StringDecoder将其转换为字符串
                                channel.pipeline().addLast("handler1", new LengthFieldBasedFrameDecoder(32,0,4,0,4));
                                channel.pipeline().addLast(new LoggingHandler(LogLevel.INFO));
                                //7，在处理管线中添加自定义的业务处理器。可指定其执行的WorkGroup
                                channel.pipeline().addLast(group, "handler2",
                                        new ChannelInboundHandlerAdapter() {
                                            //当channel触发读事件，执行该方法
                                            @Override
                                            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                                //此处的msg就是由StringDecoder处理后返回的String
                                                System.err.println(String.valueOf(msg));
                                            }
                                        });
                                channel.pipeline().addLast(new LoggingHandler(LogLevel.WARN));
                            }
                            //8,绑定监听端口
                        }).bind(8080);

        Thread.sleep(10000000);
    }

    @Test
    void client() throws InterruptedException {

        EventLoopGroup group = new DefaultEventLoopGroup();
        NioEventLoopGroup nioGroup = new NioEventLoopGroup();

        // 1，启动类
        ChannelFuture channelFuture = new Bootstrap()
                //添加EventLoop
                .group(nioGroup)
                //选择客户端的channel实现
                .channel(NioSocketChannel.class)
                //添加处理器。无论收发数据，最终都会经过该步骤
                .handler(
                        //初始化处理器在连接建立后会调用一次其中的initChannel方法
                        new ChannelInitializer<NioSocketChannel>() {
                            @Override
                            protected void initChannel(NioSocketChannel nioSocketChannel) throws Exception {
                                //添加编码器，将String类型转为字节流
                                nioSocketChannel.pipeline().addLast(new StringEncoder());
                            }

                        }).connect(new InetSocketAddress("localhost", 8080));
        Channel channel = channelFuture.channel();
        new Thread(() -> {
//            Scanner scanner = new Scanner(System.in);
            while (true) {
                byte[] msg = getMessage();
                int len = msg.length;
                ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
                buf.writeInt(len).writeBytes(msg);
//                if ("q".equals(msg)) {
//                    //close方法也是异步的
//                    channel.close().addListener(new ChannelFutureListener() {
//                        @Override
//                        public void operationComplete(ChannelFuture future) throws Exception {
//                            System.out.println("连接被用户关闭");
//                            //及时释放线程资源。调用shutdownGracefully()后，group会停止接受新的请求，并尽量处理完所有任务，最后停止
//                            group.shutdownGracefully();
//                            nioGroup.shutdownGracefully();
//                        }
//                    });
//                    break;
//                }
                //获取连接对象
                channel.writeAndFlush(buf);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }, "in").start();

        //或阻塞主线程，等待连接关闭...
        //channel.closeFuture().sync();
        //System.out.println("连接被用户关闭");

        Thread.sleep(10000);
    }


     byte[] getMessage(){
       return "message".getBytes();
    }

    void testFuture() {
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        Future<Integer> submit = executorService.submit(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                Thread.sleep(500);
                return 50;
            }
        });
    }

    @Test
    void testNettyFuture() {
        EventLoopGroup group = new NioEventLoopGroup();

        EventLoop loop = group.next();
        //此处返回的Future为Netty的拓展Future
        io.netty.util.concurrent.Future<Integer> future = loop.submit((Callable<Integer>) () -> {
            Thread.sleep(1000);
            return 60;
        });
        //Netty的Future可以一个添加回调任务
        future.addListener((f) -> {
            System.out.println(f.get());
        });
    }

    @Test
    void testPromise() throws ExecutionException, InterruptedException {
        EventLoop loop = new NioEventLoopGroup().next();
        //promise可以看作一个结果容器,它的创建需要一个EventLoop，该eventLoop用于监听promise的执行结果
        DefaultPromise<Integer> promise = new DefaultPromise<>(loop);

        //promise的运算过程可以交给任意线程执行
        new Thread(
                () -> {
                    try {
                        // 执行任务...
                        promise.setSuccess(10086);
                    } catch (Exception e) {
                        //出现异常也可以填入promise
                        e.printStackTrace();
                        promise.setFailure(e);
                    }
                }).start();

        //当结果计算完成后，可以从promise中获取。get会抛出执行时出现的异常
        System.out.println("result=" + promise.get());
    }

    @Test
    void testBuf() {
        //该方法默认返回直接内存
        ByteBuf directBuf1 = ByteBufAllocator.DEFAULT.buffer();
        //Netty默认使用直接内存
        ByteBuf directBuf2 = ByteBufAllocator.DEFAULT.directBuffer();
        //显式声明使用堆内存
        ByteBuf heapBuf = ByteBufAllocator.DEFAULT.heapBuffer();

        CompositeByteBuf buf = ByteBufAllocator.DEFAULT.compositeBuffer();
        buf.release();
    }

    @Test
    void testLengthFieldDecoder() {
        EmbeddedChannel channel = new EmbeddedChannel(
                //长度信息占4位,处理完成后再去掉头四位
                new LengthFieldBasedFrameDecoder(1024, 0, 4,0,4),
//                new LoggingHandler(LogLevel.DEBUG),
                new ChannelInboundHandlerAdapter(){
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        log.warn(String.valueOf(msg));
                        super.channelRead(ctx, msg);
                    }
                }
        );

        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
        byte[] msg =  ("Hello,world").getBytes() ;
        int len = msg.length;
        //写入长度及信息
        buf.writeInt(len);
        buf.writeBytes(msg);

        channel.writeAndFlush(buf);
    }

    public static void main(String[] args) {
        EmbeddedChannel channel = new EmbeddedChannel(
//                new ChannelInboundHandlerAdapter(){
//                    @Override
//                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//                        log.warn(String.valueOf(msg));
//                        System.out.println(msg);
//                        super.channelRead(ctx, msg);
//                    }
//                },
                new LoggingHandler(LogLevel.WARN),
                //长度信息占4位,处理完成后再去掉头四位
                new LengthFieldBasedFrameDecoder(16, 0, 4,0,4),
                new LoggingHandler(LogLevel.DEBUG)
        );

        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
        byte[] msg =  ("Hello,world").getBytes() ;
        int len = msg.length;
        //写入长度及信息
        buf.writeInt(len);
        buf.writeBytes(msg);

        channel.writeAndFlush(buf);
    }

    void testHttpServer() throws InterruptedException {
        NioEventLoopGroup bossGroup = new NioEventLoopGroup();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        ServerBootstrap boot = new ServerBootstrap();
        boot.channel(NioServerSocketChannel.class);
        boot.group(bossGroup,workerGroup);
        boot.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                 ch.pipeline().addLast(new LoggingHandler(LogLevel.INFO));
                 //HttpServerCodec http请求编解码器，继承于CombinedChannelDuplexHandler，对入栈及出栈的请求都生效
                 ch.pipeline().addLast(new HttpServerCodec());


                ch.pipeline().addLast(new SimpleChannelInboundHandler<HttpRequest>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, HttpRequest msg) throws Exception {
                        //处理请求...

                        //返回响应
                        DefaultFullHttpResponse response = new DefaultFullHttpResponse(msg.protocolVersion(),HttpResponseStatus.OK);
                        byte[] message = "<h1>hello WORLD!<h1>".getBytes();
                        int len =message.length;
                        //设置响应头，告诉浏览器响应数据长度
                        response.headers().setInt(CONTENT_LENGTH,len);
                        //将数据写入响应体
                        response.content().writeBytes(message);

                        ctx.writeAndFlush(response);
                    }
                });
                ch.pipeline().addLast(new SimpleChannelInboundHandler<HttpContent>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, HttpContent msg) throws Exception {
                        //...
                    }
                });
            }
        });
        ChannelFuture future = boot.bind(8080).sync();
        future.channel().closeFuture().sync();
    }


}


