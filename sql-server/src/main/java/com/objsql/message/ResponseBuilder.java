package com.objsql.message;

import io.netty.buffer.ByteBuf;

public class ResponseBuilder {

    private ByteBuf buf;

    private int bytesWritten = 0;

    public static ResponseBuilder get(ByteBuf buf) {
        ResponseBuilder builder = new ResponseBuilder();
        builder.buf = buf;
        return builder;
    }

    public ResponseBuilder type(byte type) {
        buf.writeByte(type);
        bytesWritten += 1;
        return this;
    }

    public ResponseBuilder addInt(int num) {
        buf.writeInt(num);
        bytesWritten += 4;
        return this;
    }

    public ResponseBuilder addByte(byte b) {
        buf.writeByte(b);
        bytesWritten += 1;
        return this;
    }

    /**
     * 写入一段带长度的数据
     */
    public ResponseBuilder addPart(byte[] bytes) {
        buf.writeInt(bytes.length).writeBytes(bytes);
        bytesWritten += ( 4 + bytes.length);
        return this;
    }

    /**
     * 获得写入的字节数
     */
    public int getBytesWritten() {
        return bytesWritten;
    }


}
