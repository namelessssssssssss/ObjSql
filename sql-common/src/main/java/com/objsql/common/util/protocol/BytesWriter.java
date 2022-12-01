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
//        if (rawIndex instanceof Integer) {
//            buf.writeInt(Integer.SIZE);
//            buf.writeInt((Integer) rawIndex);
//        } else if (rawIndex instanceof Double) {
//            buf.writeInt(Double.SIZE);
//            buf.writeDouble((Double) rawIndex);
//        } else if (rawIndex instanceof Long) {
//            buf.writeInt(Long.SIZE);
//            buf.writeLong((long) rawIndex);
//        } else if (rawIndex instanceof Boolean) {
//            buf.writeInt(1);
//            buf.writeBoolean((Boolean) rawIndex);
//        } else if (rawIndex instanceof Short) {
//            buf.writeInt(Short.SIZE);
//            buf.writeShort((Short) rawIndex);
//        } else if (rawIndex instanceof Float) {
//            buf.writeInt(Float.SIZE);
//            buf.writeFloat((Float) rawIndex);
//        } else if (rawIndex instanceof String) {
//            byte[] b = ((String) rawIndex).getBytes();
//            buf.writeInt(b.length);
//            buf.writeBytes(b);
//        } else if (rawIndex instanceof Serialized) {
//            byte[] b = ((Serialized) buf).serialize();
//            buf.writeInt(b.length);
//            buf.writeBytes(b);
//        } else {
//            throw new RuntimeException("非基本类型的key需要实现Serialized接口");
//        }
    }

}
