package com.objsql.common.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author nameless
 */
public class ExceptionUtil {

    /**
     *  获取String类型异常堆栈跟踪信息
     */
    public static String getStackTrace(Throwable exception){
        StringWriter stringWriter = new StringWriter();
        exception.printStackTrace(new PrintWriter(stringWriter, true));
        return stringWriter.getBuffer().toString();
    }
}
