package com.objsql.client.message;


import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class RawServerResponse {
    /**
     * 响应请求的类型
     */
    private byte responseType;
    /**
     * 响应序列号，对应于请求序列号
     */
    private int sequenceId;
    /**
     * 原始响应信息
     */
    private byte[] rawData;
    /**
     * 响应信息序列化方式
     */
    private byte serializeType;
    /**
     * 额外的响应信息
     */
    private String message;


}
