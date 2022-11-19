package com.objsql.client.message;

import com.objsql.common.codec.Codec;
import com.objsql.common.codec.ObjectStreamCodec;
import com.objsql.common.message.TableCreateParam;
import io.netty.buffer.ByteBuf;


/**
 * 构建客户端命令
 */
public class CommandBuilder {


    /**
     * 连接到某个表
     */
    public static int connect(String tableName, ByteBuf buf) {
        return  writeTableNameWithLength(tableName, buf);
    }

    /**
     * 索引Class默认使用java序列化
     */
    private static Codec objectStreamCodec = new ObjectStreamCodec();

    /**
     * 创建一个表
     * 请求结构：
     * ---------------
     * |head|len(tableCreateParam)|tableCreateParam|len(indexClassStream)|indexClassStream
     * ---------------
     */
    public static int create(TableCreateParam param, Codec codec, ByteBuf buf) throws Exception {
        return writeBytesWithLength(codec.encodeMessage(param), buf) + writeBytesWithLength(objectStreamCodec.encodeMessage(param.getIndexClass()), buf);
    }

    /**
     * 获取表中数据
     */
    public static int get(String tableName, byte[] key, ByteBuf buf) {
        return writeTableNameWithLength(tableName, buf) + writeKeyWithLength(key, buf);
    }

    /**
     * 获取某一数据段的某个数据
     */
    public static int getBySeg(String tableName, int segmentId, int place, ByteBuf buf) {
        int len = writeTableNameWithLength(tableName, buf);
        buf.writeInt(segmentId).writeInt(place);
        return len + 4 + 4;
    }


    /**
     * 请求在指定表中插入一条数据
     * 结构:
     * ------------------------------------
     * |head|len(tableName)|tableName|len(rawIndex)|rawIndex|len(data)|data|
     * ------------------------------------
     */
    public static int insert(String tableName, byte[] key, byte[] data, ByteBuf buf) {
        //len(tableName) + tableName
        return writeTableNameWithLength(tableName, buf)
                //len(rawIndex) + rawIndex
                + writeKeyWithLength(key, buf)
                //len(data) + data
                + writeDataWithLength(data, buf) ;
    }

    public static int update(String tableName, byte[] key, byte[] data, ByteBuf buf) {
        return insert(tableName, key, data, buf);
    }

    public static int delete(String tableName, byte[] key, ByteBuf buf) {
        return writeTableNameWithLength(tableName, buf) + writeKeyWithLength(key, buf) ;
    }

    public static int ping(ByteBuf buf){
        return 0;
    }


    public static int writeTableNameWithLength(String tableName, ByteBuf buf) {
        byte[] bytes = tableName.getBytes();
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
        return bytes.length + 4;
    }

    public static int drop(String tableName, ByteBuf buf) {
        return writeTableNameWithLength(tableName, buf);
    }

    /**
     * 写入一条带长度的信息： ....|len(data)|data|
     */
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
