package com.objsql.session;

import io.netty.channel.Channel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemorySession implements Session {

    private final Map<Integer, Channel> clientIdChannelMap = new ConcurrentHashMap<>();
    private final Map<Channel, Integer> channelClientIdMap = new ConcurrentHashMap<>();
    private final Map<Channel, Map<String, Object>> channelAttributesMap = new ConcurrentHashMap<>();


    @Override
    public void bind(Channel channel, int clientId) {
        clientIdChannelMap.put(clientId, channel);
        channelClientIdMap.put(channel, clientId);
        channelAttributesMap.put(channel, new ConcurrentHashMap<>());
    }

    @Override
    public void unbind(Channel channel) {
        int clientId = channelClientIdMap.remove(channel);
        clientIdChannelMap.remove(clientId);
        channelAttributesMap.remove(channel);
    }

    /**
     * 获取channel关联的属性
     *
     * @param channel channel
     * @param name    属性名
     */
    @Override
    public Object getAttribute(Channel channel, String name) {
        return channelAttributesMap.get(channel).get(name);
    }

    /**
     * 设置与某个channel相关联的属性
     */
    @Override
    public void setAttribute(Channel channel, String name, Object value) {
        channelAttributesMap.get(channel).put(name, value);
    }

    /**
     * 获取客户端id对于的channel
     *
     * @param clientId 客户端id
     * @return channel
     */
    @Override
    public Channel getChannel(int clientId) {
        return null;
    }
}
