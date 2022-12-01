package com.objsql.common.util.protocol;

import com.objsql.common.message.HeadMessage;
import com.objsql.common.util.common.Assert;
import io.netty.buffer.ByteBuf;

import static com.objsql.common.protocol.constants.ProtocolConstants.*;

public class HeadCodec {

    public static HeadMessage getHeader(ByteBuf buf) throws IllegalAccessException {
        int magic = buf.readInt();
        byte version = buf.readByte();
        Assert.isTrue(magic == MAGIC && version == VERSION,"协议及或版本号校验失败");
        byte serializeType = buf.readByte();
        int sequenceId = buf.readInt();
        byte messageType = buf.readByte();
        buf.readByte();
        int bodyLen = buf.readInt();
        return new HeadMessage(magic,version,serializeType,sequenceId,messageType,bodyLen);
    }


    public static void writeHeader(ByteBuf buf,HeadMessage message){
        buf.writeInt(message.getMagic());
        buf.writeByte(message.getVersion());
        buf.writeByte(message.getSerializeType());
        buf.writeInt(message.getSequenceId());
        buf.writeByte(message.getMessageType());
        buf.writeByte(0xff);
        buf.writeInt(message.getBodyLen());
    }

    public static void writeBackBodyLength(ByteBuf buf,int len){
        buf.setInt(BODY_LENGTH_OFFSET,len);
    }

}
