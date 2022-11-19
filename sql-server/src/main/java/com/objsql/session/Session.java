package com.objsql.session;

import io.netty.channel.Channel;

/**
 * 会话管理接口
 */
public interface Session {

    void bind(Channel channel,int clientId);

    void unbind(Channel channel);

    /**
     * 获取channel关联的属性
     * @param channel channel
     * @param name 属性名
     */
    Object getAttribute(Channel channel,String name);
    /**
     * 设置与channel关联的属性
     * @param channel channel
     * @param name 属性名
     */
    void setAttribute(Channel channel,String name,Object value);

    /**
     * 获取客户端id对于的channel
     * @param clientId 客户端id
     * @return channel
     */
    Channel getChannel(int clientId);
}
