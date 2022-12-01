package com.objsql.client.util;

import com.objsql.client.util.annotation.Remote;
import io.netty.channel.Channel;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class RPCProxyCreator {

    private final Channel remote;

    public RPCProxyCreator(Channel remote) {
        this.remote = remote;
    }

    @SuppressWarnings("unchecked")
    public <T> T create(Class<T> interfaceClass) {
        Method[] methods = interfaceClass.getMethods();
        List<Method> needToCreate =
                Arrays.stream(methods)
                        .filter(method -> method.getAnnotation(Remote.class) != null)
                        .collect(Collectors.toList());
        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(),new Class[]{interfaceClass},
                (proxy,method,args)->{
                      if(needToCreate.contains(method)){
                          method.getParameterTypes();
                          method.getReturnType();
                          //远程调用...
                          return null;
                      }
                      else {
                          return method.invoke(proxy, args);
                      }
                }
        );
    }
}
