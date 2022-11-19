package message.codec;

import com.objsql.client.codec.BaseClientCodec;

import com.objsql.common.codec.ObjectStreamCodec;
import com.objsql.common.protocol.constants.SerializeType;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class ClientBootRequestCodecTest {
    @Test
    void test() throws Exception {
        BaseClientCodec.register(SerializeType.OBJ_STREAM,new ObjectStreamCodec());

        EmbeddedChannel channel = new EmbeddedChannel(
                new LengthFieldBasedFrameDecoder(1024,11,4,0,0),
                new LoggingHandler(LogLevel.INFO),new BaseClientCodec(),new LoggingHandler(LogLevel.INFO)
        );


//        Command command = new Command("update table test01".getBytes(),(byte) 1,1,new Byte("0"));
//        channel.writeOutbound(command);
//
//        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
//        new BaseClientCodec().encode(null,command,buf);
//
//        channel.writeInbound(buf);
    }
}