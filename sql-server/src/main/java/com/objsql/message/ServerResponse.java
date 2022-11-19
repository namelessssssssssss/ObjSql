package com.objsql.message;

import lombok.Data;
import lombok.experimental.Accessors;

import static com.objsql.common.protocol.constants.MessageTypes.*;


@Data
@Accessors(chain = true)
public class ServerResponse {

    /**
     * 响应类型
     */
    private byte responseType;

    /**
     * 响应的请求序号
     */
    private int sequenceId;

    /**
     * 请求体序列化方式
     */
    private byte serializeType;

    private Object index;

    private byte[] data;

    /**
     * 附加错误信息
     */
    private String message;

    public static ServerResponse connect(int sequenceId, byte serializeType) {
        return new ServerResponse().setSequenceId(sequenceId).setSerializeType(serializeType).setResponseType(CONNECT);
    }

    public static ServerResponse create(int sequenceId, byte serializeType) {
        return new ServerResponse().setSequenceId(sequenceId).setSerializeType(serializeType).setResponseType(CREATE);
    }

    public static ServerResponse get(int sequenceId, byte serializeType, byte[] data) {
        return new ServerResponse().setSequenceId(sequenceId).setData(data).setSerializeType(serializeType).setResponseType(GET);
    }

    public static ServerResponse getBySeg(int sequenceId, byte serializeType, byte[] data) {
        return new ServerResponse().setSequenceId(sequenceId).setData(data).setSerializeType(serializeType).setResponseType(GET_BY_SEG_ID);
    }

    public static ServerResponse insert(int sequenceId, byte serializeType) {
        return new ServerResponse().setSequenceId(sequenceId).setSerializeType(serializeType).setResponseType(INSERT);
    }

    public static ServerResponse update(int sequenceId, byte serializeType) {
        return new ServerResponse().setSequenceId(sequenceId).setSerializeType(serializeType).setResponseType(UPDATE);
    }

    public static ServerResponse delete(int sequenceId, byte serializeType) {
        return new ServerResponse().setSequenceId(sequenceId).setSerializeType(serializeType).setResponseType(DELETE);
    }

    public static ServerResponse drop(int sequenceId, byte serializeType) {
        return new ServerResponse().setSequenceId(sequenceId).setSerializeType(serializeType).setResponseType(DROP);
    }

    public static ServerResponse exception(int sequenceId, byte serializeType, String msg) {
        return new ServerResponse().setSequenceId(sequenceId).setSerializeType(serializeType).setMessage(msg).setResponseType(EXCEPTION);
    }

    public static ServerResponse pong(int sequenceId,byte serializeType) {
        return new ServerResponse().setSequenceId(sequenceId).setSerializeType(serializeType).setResponseType(BEAT);
    }
}
