package com.objsql.client.message;

import com.objsql.common.codec.Codec;
import com.objsql.common.message.TableCreateParam;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

import static com.objsql.common.util.protocol.ByteCodeWriter.getClassBytes;
import static com.objsql.common.util.protocol.BytesWriter.*;


/**
 * 构建客户端命令
 */
public class CommandBuilder {


    /**
     * 连接到某个表
     */
    public static int connect(String tableName, ByteBuf buf) {
        return writeTableNameWithLength(tableName, buf);
    }


    /**
     * 创建一个表
     * 请求结构：
     * ---------------
     * |head|len(tableCreateParam)|tableCreateParam|len(indexClass)|indexClassStream|len(dataClass)|dataClassStream
     * ---------------
     */
    public static int create(TableCreateParam param, Codec codec, ByteBuf buf) throws Exception {
        return writeBytesWithLength(codec.encodeMessage(param), buf) +
                writeBytesWithLength(getClassBytes(param.getIndexClass()),buf) +
                writeBytesWithLength(getClassBytes(param.getDataClass()),buf);
    }



    /**
     * 获取表中数据
     */
    public static int get(String tableName, byte[] key, ByteBuf buf) {
        return writeTableNameWithLength(tableName, buf) + writeKeyWithLength(key, buf);
    }

    /**
     * 非索引字段查找
     */
    public static int getByField(String tableName, byte[] fieldKey, String fieldName, ByteBuf buf) {
        return  writeTableNameWithLength(tableName, buf) +
                writeBytesWithLength(fieldKey, buf) +
                writeBytesWithLength(fieldName.getBytes(StandardCharsets.UTF_8), buf);
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
                + writeDataWithLength(data, buf);
    }

    public static int update(String tableName, byte[] key, byte[] data, ByteBuf buf) {
        return insert(tableName, key, data, buf);
    }

    public static int delete(String tableName, byte[] key, ByteBuf buf) {
        return writeTableNameWithLength(tableName, buf) + writeKeyWithLength(key, buf);
    }

    public static int ping(ByteBuf buf) {
        return 0;
    }




    public static int drop(String tableName, ByteBuf buf) {
        return writeTableNameWithLength(tableName, buf);
    }

    /**
     * 写入一条带长度的信息： ....|len(data)|data|
     */


}
