package com.objsql.common.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class BytesReader {

    /**
     * 读取带长度的一段数据
     */
    public static byte[] readPart(ByteBuf buf){
        int len = buf.readInt();
        return ByteBufUtil.getBytes(buf.readBytes(len));
    }

    /**
     * 读取一段带长度的数据，以指定字符集解码为String
     */
    public static String readStringPart(ByteBuf buf, Charset charset){
        return new String(readPart(buf),charset);
    }

    /**
     * 读取一段带长度的数据，以UTF-16解码为String
     */
    public static String readStringPart(ByteBuf buf){
        return readStringPart(buf,StandardCharsets.UTF_16);
    }
}
