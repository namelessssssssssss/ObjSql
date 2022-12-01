package com.objsql.common.protocol.codec;

import com.objsql.common.protocol.constants.ProtocolConstants;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * 自定义协议帧解码器
 */
public class ProtocolFrameDecoder extends LengthFieldBasedFrameDecoder {

    public ProtocolFrameDecoder(){
        this(40960,
                ProtocolConstants.BODY_LENGTH_OFFSET,
                ProtocolConstants.BODY_LENGTH_MSG_LENGTH,
                0,
                0
        );
    }

    public ProtocolFrameDecoder(int maxFrameLength, int lengthFieldOffset, int lengthFieldLength, int lengthAdjustment, int initialBytesToStrip) {
        super(maxFrameLength, lengthFieldOffset, lengthFieldLength, lengthAdjustment, initialBytesToStrip);
    }
}
