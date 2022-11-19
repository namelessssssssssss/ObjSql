package com.objsql.db;

import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.JSONToken;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;
import lombok.Data;
import lombok.experimental.Accessors;

import java.lang.reflect.Type;

@Data
@Accessors(chain = true)
/**
 *  定义data的反序列化逻辑
 *  @author nameless
 */
public class DataDeserializer implements ObjectDeserializer {

    private Class<?> dataClass;

    private Class<?> indexClass;

    @Override
    public <T> T deserialze(DefaultJSONParser parser, Type type, Object fieldName) {
     //        "e1".equals((String)fieldName) ? parser.parseObject(dataClass) : null
     return null;
    }

    @Override
    public int getFastMatchToken() {
        return JSONToken.LITERAL_INT;
    }
}
