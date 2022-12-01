package com.objsql.common.util.common;

public class Assert {

    public static void isTrue(boolean condition,String message) throws IllegalAccessException {
        if(!condition){
            throw new IllegalAccessException(message);
        }
    }

}
