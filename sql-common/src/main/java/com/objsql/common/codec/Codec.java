package com.objsql.common.codec;

/**
 * 消息体序列化使用的编解码器
 */
public interface Codec {

    byte[] encodeMessage(Object body) throws Exception;


    Object decodeBody(byte[] bytes, Class... messageAndGenericClass) throws Exception;

    int getSerializeType();

}
