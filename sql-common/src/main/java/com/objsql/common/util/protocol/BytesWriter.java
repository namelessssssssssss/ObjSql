package com.objsql.common.util.protocol;

import io.netty.buffer.ByteBuf;

public class BytesWriter {

    public static int writeTableNameWithLength(String tableName, ByteBuf buf) {
        byte[] bytes = tableName.getBytes();
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
        return bytes.length + 4;
    }

    public static int writeBytesWithLength(byte[] data, ByteBuf buf) {
        buf.writeInt(data.length).writeBytes(data);
        return data.length + 4;
    }

    public static int writeDataWithLength(byte[] data, ByteBuf buf) {
        return writeBytesWithLength(data, buf);
    }

    public static int writeKeyWithLength(byte[] key, ByteBuf buf) {
        return writeBytesWithLength(key, buf);
    }

}
