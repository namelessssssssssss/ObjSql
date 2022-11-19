
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.Data;
import lombok.experimental.Accessors;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@SuppressWarnings("all")
public class NioTest {
    @Test
    void testChannel() throws Exception {
        try (
                FileChannel channel = new FileInputStream("C:\\Users\\nameless\\Pictures\\Capture.png").getChannel();
        ) {
            channel.size();
        }

    }

    private static String writeBufName = "write";
    private static String readBufName = "read";

    @Data
    @Accessors(chain = true)
    static class Worker implements Runnable {
        private Thread thread;
        private Selector selector;
        private String name;
        private volatile boolean inited = false;
        //使用队列作为两个线程间传递信息的通道
        public final ConcurrentLinkedDeque<Function> queue = new ConcurrentLinkedDeque();

        public Worker(String name) {
            this.name = name;
        }

        //初始化线程和selector
        public void register(SocketChannel channel) throws IOException {
            if (!inited) {
                thread = new Thread(this, name);
                selector = Selector.open();
                inited = true;
                thread.start();
            }

            //注册任务交给worker线程执行
            queue.add((val) -> {
                try {
                    channel.configureBlocking(false);
                    Map<String, ByteBuffer> buffers = new HashMap<>(2);
                    buffers.put(readBufName, ByteBuffer.allocate(128));
                    buffers.put(writeBufName, ByteBuffer.allocate(128));
                    SelectionKey key = channel.register(selector, SelectionKey.OP_READ, buffers);

                    //若建立连接时要向客户端发送一些信息,且信息比较大
                    buffers.get(writeBufName).put(new byte[128]);
                    //一次写入可能无法写完所有信息
                    channel.write(buffers.get(writeBufName));
                    //注册对可写事件的监听，当下次客户端可写时发送剩余数据。多个事件之间可以相加，表示对多个事件的监听
                    key.interestOps(key.interestOps() + SelectionKey.OP_WRITE);

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
            //使select()放行一次
            selector.wakeup();
            /**
             * 另一种简单的可行操作是：
             * selector.wakeUp();
             * channel.register...
             */
        }

        @Override
        public void run() {
            while (true) {
                try {
                    selector.select();
                    //执行注册任务
                    if (!queue.isEmpty()) {
                        queue.pop().apply(null);
                    }
                    Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                    if (iterator.hasNext()) {
                        SelectionKey key = iterator.next();
                        iterator.remove();
                        if (key.isReadable()) {
                            try {
                                int read;
                                SocketChannel clientChannel = (SocketChannel) key.channel();
                                ByteBuffer buf = ((Map<String, ByteBuffer>) key.attachment()).get(readBufName);
                                //对于较大的请求，可能一次读取无法完全读完所有数据。如果一次全部读完，不符合非阻塞的思想（影响其它事件的处理）
                                if ((read = clientChannel.read(buf)) > 0) {
                                    buf.flip();
                                    System.out.println(name + ":"+Charset.defaultCharset().decode(buf));
                                    // *处理本次请求数据*
                                    buf.clear();
                                }
                                //客户端断开连接时（无论正常断开或异常断开），都会触发一次读事件
                                //若正常断开，调用Channel#read返回-1
                                if (read == -1) {
                                    key.attach(null);
                                    key.cancel();
                                }
                            } catch (IOException e) {
                                key.attach(null);
                                //客户端异常断开（未使用SocketChannel#close关闭），关闭Selector对key对应Channel的监听（反注册）
                                key.cancel();
                            }
                        }
                        //发送剩余的，未发送的信息
                        else if (key.isWritable()) {
                            Map<String, ByteBuffer> buf = ((Map<String, ByteBuffer>) key.attachment());
                            ((SocketChannel) key.channel()).write(buf.get(writeBufName));
                            //若没有剩余消息，取消对写事件的监听，释放buffer内存
                            if (!buf.get(writeBufName).hasRemaining()) {
                                buf.remove(writeBufName);
                                key.interestOps(key.interestOps() - SelectionKey.OP_WRITE);
                            }
                        }

                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static int WORKER_AMOUNT = 5;

    @Test
    @SuppressWarnings("all")
    void testSocketServer() throws IOException {
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress(8080));
        serverSocketChannel.configureBlocking(false);
        List<SocketChannel> channels = new ArrayList<>();

        //监听器,注册了对ServerSocketChannel的监听。它可以监听多个Channel。
        Selector serverSelector = Selector.open();
        //注册选择器，并设置对ServerSocketChannel指定连接事件的监听。
        // 返回的SelectionKey负责注册的Channel上发生的事件。Selector中有一个包含所有其监听Channel的Key集合。
        SelectionKey sscKey = serverSocketChannel.register(
                serverSelector, SelectionKey.OP_ACCEPT, ByteBuffer.allocate(2048)
        );

        List<Worker> workers = new ArrayList<>(5);

        for (int c = 0; c < WORKER_AMOUNT; c++) {
            workers.add(new Worker("worker-" + c));
        }
        //请求计数器,借此轮询workers
        AtomicInteger count = new AtomicInteger();

        while (true) {
            //无事件则阻塞
            serverSelector.select();
            //有新事件，获取所有发生事件Channel的SelectionKey。
            //selector只会向selectedKeys列表中添加发生新事件channel的Key，而不会自动移除
            Iterator<SelectionKey> iterator = serverSelector.selectedKeys().iterator();
            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                //需要手动将Key从selectedKeys列表中移除
                iterator.remove();

                if (key.isAcceptable()) {
                    //拿到发生该事件的Channel
                    ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
                    //接受连接，将其加入连接列表
                    SocketChannel clientChannel = serverChannel.accept();
                    clientChannel.configureBlocking(false);
                    channels.add(clientChannel);
                    //需要注意，如果什么事情都不做（在此处是没有接受连接），该事件会视为未完成，
                    //selector.select()不会回到阻塞状态，直到当前事件处理完成 （在有事件未处理时一直为非阻塞）
                    //或者使用 SelectionKey#cancel()来取消对某个Channel的监听

                    //注册worker对SocketChannel读写事件的监听。
                    // 注意：若此时worker0正在调用selector.select()且正在阻塞，使用boss线程对channel注册也会阻塞，直到selector.select()不再阻塞。
                    workers.get(count.getAndIncrement() % WORKER_AMOUNT).register(clientChannel);
                }
            }
        }
    }


    static void testSendMessage() {

    }

    @Test
    void testSocketClient() throws IOException {
        for (int c = 0; c < 10000; c++) {
        SocketChannel channel = SocketChannel.open();
        channel.connect(new InetSocketAddress(8080));
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            buffer.put(Charset.defaultCharset().encode("hello from client:" + c));
            buffer.flip();
            channel.write(buffer);
            buffer.clear();
        }
        return;
    }

    @Test
    void testEventLoop() throws InterruptedException {
        //创建事件循环组，它继承于线程池，默认创建一定量的EventLoop线程
        NioEventLoopGroup group = new NioEventLoopGroup();
        //获取下一个事件循环对象，自动轮询
        group.next();
        //执行普通任务
        group.next().submit(()->{
            System.out.println(Thread.currentThread());
        });
        //和submit相同
        group.next().execute(()->{
            System.out.println(Thread.currentThread());
        });
        //提交执行定时任务
        group.next().scheduleAtFixedRate(()->{
            System.out.println(Thread.currentThread());
        },1,1, TimeUnit.SECONDS);
        Thread.sleep(20000);
    }
}
