package com.objsql.client.codec;

import com.objsql.client.message.ClientRequest;
import com.objsql.client.message.CommandBuilder;
import com.objsql.client.message.RawServerResponse;
import com.objsql.common.codec.Codec;
import com.objsql.common.codec.JsonCodec;
import com.objsql.common.codec.ObjectStreamCodec;
import com.objsql.common.protocol.constants.ProtocolConstants;
import com.objsql.common.protocol.util.HeadCodec;
import com.objsql.common.protocol.util.HeadMessage;
import com.objsql.common.util.BytesReader;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.objsql.common.protocol.constants.MessageTypes.*;

/**
 * 客户端基本编解码器
 */
@Slf4j
@ChannelHandler.Sharable
public class BaseClientCodec extends MessageToMessageCodec<ByteBuf, ClientRequest> {

    //校验用头信息
    private static final int MAGIC = 1324;
    //版本号
    private static final byte VERSION = 1;

    public static final Map<Byte, Codec> codecMap = new HashMap<>();

    static {
        codecMap.put((byte) 0, new JsonCodec());
        codecMap.put((byte) 1, new ObjectStreamCodec());
    }

    /**
     * 添加特定类型的编解码器
     *
     * @param type  类型编码
     * @param codec 编解码器
     */
    public static void register(Byte type, Codec codec) {

        codecMap.put(type, codec);
    }

    @Override
    public void encode(ChannelHandlerContext channelHandlerContext, ClientRequest clientRequest, List<Object> out) throws Exception {
        ByteBuf byteBuf = channelHandlerContext.alloc().buffer();
        HeadCodec.writeHeader(byteBuf,
                new HeadMessage(
                        ProtocolConstants.MAGIC,
                        ProtocolConstants.VERSION,
                        clientRequest.getSerializeType(),
                        clientRequest.getSequenceId(),
                        clientRequest.getMessageType(),
                        0
                ));

        Codec codec = codecMap.get(clientRequest.getSerializeType());
        if (codec == null) {
            throw new IllegalAccessException("未找到消息指定的编解码器");
        }
        int len = writeBody(clientRequest, codec, byteBuf);
        //写回内容长度
        HeadCodec.writeBackBodyLength(byteBuf, len);
        //写出请求
        channelHandlerContext.writeAndFlush(byteBuf);
    }

    private int writeBody(ClientRequest clientRequest, Codec codec, ByteBuf buf) throws Exception {
        if (clientRequest.getMessageType() == CONNECT) {
            return CommandBuilder.connect(clientRequest.getTableName(), buf);
        } else if (clientRequest.getMessageType() == CREATE) {
            return CommandBuilder.create(clientRequest.getTableParam(), codec, buf);
        } else if (clientRequest.getMessageType() == GET) {
            return CommandBuilder.get(clientRequest.getTableName(), codec.encodeMessage(clientRequest.getIndex()), buf);
        } else if (clientRequest.getMessageType() == GET_BY_SEG_ID) {
            return CommandBuilder.getBySeg(clientRequest.getTableName(), clientRequest.getSegmentId(), clientRequest.getPlace(), buf);
        } else if (clientRequest.getMessageType() == INSERT) {
            return CommandBuilder.insert(clientRequest.getTableName(), codec.encodeMessage(clientRequest.getIndex()), codec.encodeMessage(clientRequest.getData()), buf);
        } else if (clientRequest.getMessageType() == UPDATE) {
            return CommandBuilder.update(clientRequest.getTableName(), codec.encodeMessage(clientRequest.getIndex()), codec.encodeMessage(clientRequest.getData()), buf);
        } else if (clientRequest.getMessageType() == DELETE) {
            return CommandBuilder.delete(clientRequest.getTableName(), codec.encodeMessage(clientRequest.getIndex()), buf);
        } else if (clientRequest.getMessageType() == DROP) {
            return CommandBuilder.drop(clientRequest.getTableName(), buf);
        } else if (clientRequest.getMessageType() == BEAT) {
            return CommandBuilder.ping(buf);
        } else {
            throw new RuntimeException("不支持的操作类型:" + clientRequest.getMessageType());
        }
    }

    /**
     * 头信息，10 + 4字节
     */
    private void writeHeader(ByteBuf byteBuf, ClientRequest clientRequest) {
        //校验数,4字节
        byteBuf.writeInt(MAGIC);
        //版本号,1字节
        byteBuf.writeByte(VERSION);
        //正文序列化方式,1字节
        byteBuf.writeByte(clientRequest.getSerializeType());
        //请求序号,4字节
        byteBuf.writeInt(clientRequest.getSequenceId());
        //请求体长度，占位
        byteBuf.writeInt(0);
    }


    /**
     * 解码服务端响应信息
     */
    @Override
    public void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> in) throws IllegalAccessException {
        HeadMessage message = HeadCodec.getHeader(byteBuf);
        //解码服务器的返回信息
        RawServerResponse response = decodeBody(byteBuf,message.getMessageType());
        response.setSequenceId(message.getSequenceId());
        response.setSerializeType(message.getSerializeType());
        in.add(response);
    }

    private RawServerResponse decodeBody(ByteBuf buf,byte type) {
        RawServerResponse response = new RawServerResponse().setResponseType(type);
        switch (type) {
            case GET:
                return response.setRawData(BytesReader.readPart(buf));
            case EXCEPTION:
                return response.setMessage(BytesReader.readStringPart(buf));
            case BEAT:
            case GET_BY_SEG_ID:
            case CONNECT:
            case CREATE:
            case INSERT:
            case UPDATE:
            case DELETE:
            case DROP:
                return response;
            default:
                throw new RuntimeException("未知响应类型:" + type);
        }

    }

}
