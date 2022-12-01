package com.objsql.client.message;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

@Data
@Accessors(chain = true)
public class HandledServerResponse {

    /**
     * 索引字段查找结果
     */
    private Object data;
    /**
     * 非索引字段查找结果
     */
    private List<Object> dataList;

    /**
     * 响应异常信息
     */
    private String errorMessage;

    private String message;

    private byte serializeType;

}
