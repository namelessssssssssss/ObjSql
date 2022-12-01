package com.objsql.common.codec;

import java.io.*;

/**
 * Java对象流序列化编解码器
 */
public class ObjectStreamCodec implements Codec {

    public static final byte serializeType = 1;

    @Override
    public byte[] encodeMessage(Object body) throws IOException {
        //jdk序列化
        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        ObjectOutputStream stream = new ObjectOutputStream(byteArrayStream);
        stream.writeObject(body);
        return byteArrayStream.toByteArray();
    }

    @Override
    public Object decodeBody(byte[] bytes,Class... genericClass) throws IOException,ClassNotFoundException {

        ObjectInputStream in = new ObjectInputStream(
                new ByteArrayInputStream(bytes)
        );
        Object res = in.readObject();
        if (!res.getClass().equals(genericClass[0])) {
            throw new RuntimeException("转换后的类型与提供的类型不符");
        } else {
            return res;
        }
    }

    @Override
    public int getSerializeType() {
        return serializeType;
    }


}
