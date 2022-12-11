package com.objsql.common.util.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class BytesReader {

    /**
     * 读取带长度的一段数据
     */
    public static byte[] readPart(ByteBuf buf) {
        int len = buf.readInt();
        return len > 0 ? ByteBufUtil.getBytes(buf.readBytes(len)) : new byte[0];
    }

    /**
     * 读取一段带长度的数据，以指定字符集解码为String
     */
    public static String readStringPart(ByteBuf buf, Charset charset) {
        return new String(readPart(buf), charset);
    }

    /**
     * 读取多段连续的消息体
     */
    public static List<byte[]> readParts(ByteBuf buf) {
        List<byte[]> parts = new ArrayList<>(1);
        while (buf.readableBytes() > 0) {
            int len = buf.readInt();
            parts.add(ByteBufUtil.getBytes(buf.readBytes(len)));
        }
        return parts;
    }

    /**
     * 读取一段带长度的数据，以UTF-8解码为String
     */
    public static String readStringPart(ByteBuf buf) {
        return readStringPart(buf, StandardCharsets.UTF_8);
    }
}
