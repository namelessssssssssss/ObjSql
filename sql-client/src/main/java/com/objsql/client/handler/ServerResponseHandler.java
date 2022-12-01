package com.objsql.client.handler;

import com.objsql.client.codec.BaseClientCodec;
import com.objsql.client.message.HandledServerResponse;
import com.objsql.client.message.MissionQueue;
import com.objsql.client.message.RawServerResponse;
import com.objsql.common.codec.Codec;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.ArrayList;
import java.util.List;

import static com.objsql.common.protocol.constants.MessageTypes.*;

public class ServerResponseHandler extends SimpleChannelInboundHandler<RawServerResponse> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RawServerResponse response) throws Exception {
        HandledServerResponse handledResponse = new HandledServerResponse();
        try {
            byte responseType = response.getResponseType();
            if (responseType == GET) {
                Class<?> dataClass = MissionQueue.getRequest(response.getSequenceId()).get().getDataClass();
                Codec codec = BaseClientCodec.codecMap.get(response.getSerializeType());
                Object data = null;
                if (response.getRawData().length > 0) {
                    data = codec.decodeBody(response.getRawData(), dataClass);
                }
                handledResponse.setData(data);
            } else if (responseType == GET_BY_FIELD) {
                Class<?> dataClass = MissionQueue.getRequest(response.getSequenceId()).get().getDataClass();
                Codec codec = BaseClientCodec.codecMap.get(response.getSerializeType());
                List<Object> results = new ArrayList<>(1);
                for (byte[] res : response.getMultipartRawData()){
                    results.add(codec.decodeBody(res,dataClass));
                }
                handledResponse.setDataList(results);
            } else if (responseType == EXCEPTION) {
                handledResponse.setErrorMessage(response.getMessage());
            } else if (responseType == BEAT) {
                handledResponse.setMessage("pong!");
            }
        } finally {
            MissionQueue.finish(response.getSequenceId(), handledResponse);
        }
    }
}
