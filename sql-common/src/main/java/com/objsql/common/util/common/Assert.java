package com.objsql.common.util.common;

public class Assert {

    public static void isTrue(boolean condition,String message) throws IllegalStateException {
        if(!condition){
            throw new IllegalStateException(message);
        }
    }

}
