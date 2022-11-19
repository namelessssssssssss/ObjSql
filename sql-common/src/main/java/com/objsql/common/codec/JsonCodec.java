package com.objsql.common.codec;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.util.Arrays;

/**
 * json编解码器
 */
public class JsonCodec implements Codec {

    public static final byte serializeType = 0;
    @Override
    public byte[] encodeMessage(Object body) {
        return JSON.toJSONBytes(body);
    }

    @Override
    public Object decodeBody(byte[] bytes,Class... genericClass) {
        if (genericClass.length == 1) {
            return JSON.parseObject(bytes, genericClass[0]);
        }
        return JSON.parseObject(Arrays.toString(bytes), new TypeReference<>(genericClass) {
        });
    }

    /**
     * @return
     */
    @Override
    public int getSerializeType() {
        return serializeType;
    }


}
