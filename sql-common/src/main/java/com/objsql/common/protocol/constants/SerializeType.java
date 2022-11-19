package com.objsql.common.protocol.constants;

/**
 * 消息体（rawIndex/data）的序列化方式
 */
public class SerializeType {
    public static final byte OBJ_STREAM = 0;
    public static final byte JSON = 1;
    public static final byte PROTOBUF = 2;
}
