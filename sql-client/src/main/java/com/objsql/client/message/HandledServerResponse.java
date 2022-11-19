package com.objsql.client.message;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class HandledServerResponse {

    /**
     * 解码后的响应信息
     */
    private Object data;

    /**
     * 响应异常信息
     */
    private String errorMessage;

    private String message;

    private byte serializeType;

}
