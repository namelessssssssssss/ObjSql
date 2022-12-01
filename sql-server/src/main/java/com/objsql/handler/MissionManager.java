package com.objsql.handler;

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.Promise;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public class MissionManager {

    private static final Map<Integer, Promise<?>> promises = new ConcurrentHashMap<>();

    private static final ExecutorService promiseExecutor = new DefaultEventExecutorGroup(5);

    public static Promise submit(Promise<?> promise){

        return null;
    }
}
