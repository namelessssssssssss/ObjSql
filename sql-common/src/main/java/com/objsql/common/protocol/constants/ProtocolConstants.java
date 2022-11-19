package com.objsql.common.protocol.constants;

public class ProtocolConstants {

    public static final int MAGIC_LEN = 4;

    public static final int VERSION_LEN = 1;

    public static final int SERIALIZE_TYPE_LEN = 1;

    public static final int SEQUENCE_ID_LEN = 4;

    public static final int MESSAGE_TYPE_LEN = 1;

    public static final int PLACE_HOLDER_LENGTH = 1;

    public static final int BODY_LENGTH_MSG_LENGTH = 4;

    /**
     * 消息体长度字段偏移量
     */
    public static final int BODY_LENGTH_OFFSET =
            MAGIC_LEN + VERSION_LEN + SERIALIZE_TYPE_LEN + SEQUENCE_ID_LEN + MESSAGE_TYPE_LEN + PLACE_HOLDER_LENGTH;

    /**
     * 头信息长度
     */
    public static final int HEADER_LENGTH = BODY_LENGTH_OFFSET + BODY_LENGTH_MSG_LENGTH;

    //校验用头信息
    public static final int MAGIC = 1919810;
    //版本号
    public static final byte VERSION = 1;
}
