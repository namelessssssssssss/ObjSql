package com.objsql.client.handler;

import com.objsql.client.codec.BaseClientCodec;
import com.objsql.client.message.HandledServerResponse;
import com.objsql.client.message.MissionQueue;
import com.objsql.client.message.RawServerResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import static com.objsql.common.protocol.constants.MessageTypes.*;

public class ServerResponseHandler extends SimpleChannelInboundHandler<RawServerResponse> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RawServerResponse response) throws Exception {
        HandledServerResponse handledResponse = new HandledServerResponse();
        byte responseType  = response.getResponseType();
        if (responseType == GET) {
            Class<?> dataClass = MissionQueue.getRequest(response.getSequenceId()).get().getDataClass();
            Object data = BaseClientCodec.codecMap.get(response.getSerializeType()).decodeBody(response.getRawData(), dataClass);
            handledResponse.setData(data);
        }
        else if(responseType == EXCEPTION){
            handledResponse.setErrorMessage(response.getMessage());
        }
        else if(responseType == BEAT){
            handledResponse.setMessage("pong!");
        }
        MissionQueue.finish(response.getSequenceId(),handledResponse);
    }
}
