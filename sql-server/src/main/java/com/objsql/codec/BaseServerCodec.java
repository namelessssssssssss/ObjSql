package com.objsql.codec;


import com.objsql.common.codec.Codec;
import com.objsql.common.codec.JsonCodec;
import com.objsql.common.codec.ObjectStreamCodec;
import com.objsql.common.protocol.constants.ProtocolConstants;
import com.objsql.common.protocol.util.HeadCodec;
import com.objsql.common.protocol.util.HeadMessage;
import com.objsql.message.RawClientRequest;
import com.objsql.message.ResponseBuilder;
import com.objsql.message.ServerResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.objsql.common.protocol.constants.MessageTypes.*;


@ChannelHandler.Sharable
public class BaseServerCodec extends MessageToMessageCodec<ByteBuf, ServerResponse> {

    //校验用头信息
    private static final int MAGIC = 1324;
    //版本号
    private static final byte VERSION = 1;
    //数据体的编解码器
    public static final Map<Byte, Codec> codecMap = new HashMap<>();

    static {
        codecMap.put(JsonCodec.serializeType, new JsonCodec());
        codecMap.put(ObjectStreamCodec.serializeType, new ObjectStreamCodec());
    }

    /**
     * 编码服务器返回响应信息
     */
    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, ServerResponse serverResponse, List<Object> out) throws Exception {
        ByteBuf byteBuf = channelHandlerContext.alloc().buffer();
        //写入头信息
        HeadCodec.writeHeader(byteBuf, new HeadMessage(
                ProtocolConstants.MAGIC,
                ProtocolConstants.VERSION,
                serverResponse.getSerializeType(),
                serverResponse.getSequenceId(),
                serverResponse.getResponseType(),
                0
        ));
        //写入消息体
        int len = writeBody(serverResponse, byteBuf);
        //写回消息体长度
        HeadCodec.writeBackBodyLength(byteBuf, len);
        //返回信息
        channelHandlerContext.writeAndFlush(byteBuf);
    }

    //TODO : data序列化
    private int writeBody(ServerResponse serverResponse, ByteBuf buf) throws Exception {
        switch (serverResponse.getResponseType()) {
            case CONNECT:
                return ResponseBuilder.get(buf).getBytesWritten();
            case CREATE:
                return ResponseBuilder.get(buf).getBytesWritten();
            case GET:
                return ResponseBuilder.get(buf).addPart(serverResponse.getData()).getBytesWritten();
            case GET_BY_SEG_ID:
                return -1;
            case INSERT:
                return ResponseBuilder.get(buf).getBytesWritten();
            case UPDATE:
                return ResponseBuilder.get(buf).getBytesWritten();
            case DELETE:
                return ResponseBuilder.get(buf).getBytesWritten();
            case DROP:
                return ResponseBuilder.get(buf).getBytesWritten();
            case EXCEPTION:
                return ResponseBuilder.get(buf).addPart(serverResponse.getMessage().getBytes(StandardCharsets.UTF_16)).getBytesWritten();
            case BEAT:
                return ResponseBuilder.get(buf).getBytesWritten();
            default:
                throw new NoSuchMethodException("不支持的操作类型:" + serverResponse.getResponseType());
        }
    }

    /**
     * 解码客户端请求
     */
    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf buf, List<Object> in) throws Exception {
        HeadMessage message = HeadCodec.getHeader(buf);
        Codec codec = codecMap.get(message.getSerializeType());
        if (codec == null) {
            throw new RuntimeException("不支持的编码方式:" + message.getSerializeType());
        }
        RawClientRequest request = decodeBody(buf, message.getMessageType());
        request.setSequenceId(message.getSequenceId());
        request.serializeType(message.getSerializeType());
        in.add(request);
    }

    private RawClientRequest decodeBody(ByteBuf body, byte commandType) throws Exception {
        if (commandType == CONNECT) {
            return new RawClientRequest().connect().tableName(readTableNameWithLength(body)).finish();
        } else if (commandType == CREATE) {
            return new RawClientRequest().create().table(readBodyWithLength(body)).rawIndexClass(readBodyWithLength(body)).finish();
        } else if (commandType == GET) {
            return new RawClientRequest().get().tableName(readTableNameWithLength(body)).index(readKeyWithLength(body)).finish();
        } else if (commandType == GET_BY_SEG_ID) {
            return new RawClientRequest().getBySeg().tableName(readTableNameWithLength(body)).segmentId(body.readInt()).place(body.readInt()).finish();
        } else if (commandType == INSERT) {
            return new RawClientRequest().insert().tableName(readTableNameWithLength(body)).rawIndex(readKeyWithLength(body)).data(readBodyWithLength(body)).finish();
        } else if (commandType == UPDATE) {
            return new RawClientRequest().update().tableName(readTableNameWithLength(body)).rawIndex(readKeyWithLength(body)).data(readBodyWithLength(body)).finish();
        } else if (commandType == DELETE) {
            return new RawClientRequest().delete().tableName(readTableNameWithLength(body)).rawIndex(readKeyWithLength(body)).finish();
        } else if (commandType == DROP) {
            return new RawClientRequest().drop().tableName(readTableNameWithLength(body)).finish();
        } else if (commandType == BEAT) {
            return new RawClientRequest().ping().finish();
        } else {
            throw new IllegalAccessException("不支持的操作类型:" + commandType);
        }
    }

    /**
     * int转byte[],高字节序
     */
    public static byte[] getBytes(int n) {
        byte[] b = new byte[4];
        b[3] = (byte) (n & 0xff);
        b[2] = (byte) (n >> 8 & 0xff);
        b[1] = (byte) (n >> 16 & 0xff);
        b[0] = (byte) (n >> 24 & 0xff);
        return b;
    }

    String readTableNameWithLength(ByteBuf buf) {
        int len = buf.readInt();
        return new String(ByteBufUtil.getBytes(buf.readBytes(len)));
    }

    byte[] readKeyWithLength(ByteBuf buf) {
        return readBodyWithLength(buf);
    }

    byte[] readBodyWithLength(ByteBuf buf) {
        int len = buf.readInt();
        return ByteBufUtil.getBytes(buf.readBytes(len));
    }


}
