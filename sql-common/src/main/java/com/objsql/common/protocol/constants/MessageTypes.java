package com.objsql.common.protocol.constants;

public class MessageTypes {
    /**
     * 连接到某个表
     */
    public static final byte CONNECT = 0;
    /**
     * 创建表
     */
    public static final byte CREATE = 1;
    /**
     * 删除表
     */
    public static final byte DROP = 2;
    /**
     * 通过key获取数据
     */
    public static final byte GET = 3;
    /**
     * 通过数据段号获取数据
     */
    public static final byte GET_BY_FIELD = 4;
    /**
     * 添加一条数据
     */
    public static final byte INSERT = 5;
    /**
     * 更新一条数据
     */
    public static final byte UPDATE = 6;
    /**
     * 删除一条数据
     */
    public static final byte DELETE = 7;
    /**
     * 服务端遇到异常
     */
    public static final byte EXCEPTION = 8;
    /**
     * 客户端心跳请求
     */
    public static final byte BEAT = 9;

    /**
     * 是用户类
     */
    public static final byte IS_APP_CLASS = -1;

    /**
     * 是外部类
     */
    public static final byte IS_EXT_CLASS = -2;
}
