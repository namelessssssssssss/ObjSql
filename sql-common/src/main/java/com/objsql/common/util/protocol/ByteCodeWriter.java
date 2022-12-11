package com.objsql.common.util.protocol;

import com.objsql.common.protocol.constants.MessageTypes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;

public class ByteCodeWriter {

    /**
     * 获取协议格式的Class字节码
     */
    public static byte[] getClassBytes(Class<?> clazz){
        ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer();
        //标识该类是用户类还是外部类
        byte isAppClass = (clazz.getResource("") == null ? MessageTypes.IS_EXT_CLASS : MessageTypes.IS_APP_CLASS);
        buf.writeByte(isAppClass);
        //写入该类全类名
        BytesWriter.writeDataWithLength(clazz.getName().getBytes(StandardCharsets.UTF_8),buf);
        //若是用户类，写入该类字节码
        if(isAppClass == MessageTypes.IS_APP_CLASS) {
            try (FileInputStream out = new FileInputStream(clazz.getResource("").getPath() + clazz.getSimpleName() + ".class")){
                buf.writeBytes(out.readAllBytes());
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
        return ByteBufUtil.getBytes(buf);
    }
}
