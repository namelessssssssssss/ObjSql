package com.objsql.db.util;

import com.alibaba.fastjson.util.ParameterizedTypeImpl;
import com.objsql.db.MaxSize;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * @author nameless
 */
public class TableUtils {

    /**
     * 获取以@ByteSize估算的序列化json字符串的大小
     * @return 对象序列化后的大小，byte
     */
    public static int readObjectSize(Class<?> clazz){
        int res = 4;
        Field[] fields =clazz.getDeclaredFields();
        for(Field field:fields){
            MaxSize maxSize = field.getAnnotation(MaxSize.class);
            if(maxSize !=null){
                res+= maxSize.value() + 8;
            }
        }
        return res;
    }

    /**
     * 读取文件中的一段位置,返回空字符之前的部分。
     * @param offset 开始读取的偏移量
     * @param maxSize 读取的最大长度
     */
    public static byte[] readToEnd(FileChannel channel,long offset,int maxSize) throws IOException {
        ByteBuffer buffer =ByteBuffer.allocate(maxSize);
        channel.read(buffer,offset);
        int count = 0;
        for(byte b : buffer.array()){
            if(b != 0) {
               count++;
            }
            else {
                break;
            }
        }
        return Arrays.copyOf(buffer.array(),count);
    }

    /** 为序列化两次以上泛型提供的工具类,作用类似于
     * TypeReference< BaseResult<List<T>>> type = new TypeReference<BaseResult<List<T>>>(BaseResult.class, List.class, returnClazz) { };
     */
    public static Type buildType(Type... types) {
        ParameterizedTypeImpl beforeType = null;
        if (types != null && types.length > 0) {
            for (int i = types.length - 1; i > 0; i--) {
                beforeType = new ParameterizedTypeImpl(new Type[]{beforeType == null ? types[i] : beforeType}, null, types[i - 1]);
            }
        }
        return beforeType;
    }
}
