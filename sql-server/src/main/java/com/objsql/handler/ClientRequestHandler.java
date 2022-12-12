package com.objsql.handler;

import com.objsql.codec.BaseServerCodec;
import com.objsql.common.codec.Codec;
import com.objsql.common.codec.ObjectStreamCodec;
import com.objsql.common.message.TableCreateParam;
import com.objsql.common.util.common.ExceptionUtil;
import com.objsql.db.entity.Pair;
import com.objsql.db.Table;
import com.objsql.db.Tree;
import com.objsql.message.RawClientRequest;
import com.objsql.message.ServerResponse;
import com.objsql.session.TableMap;
import com.objsql.common.util.protocol.ByteCodeLoader;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static com.objsql.common.protocol.constants.MessageTypes.*;


@SuppressWarnings("all")
@Slf4j
@ChannelHandler.Sharable
public class ClientRequestHandler extends SimpleChannelInboundHandler<RawClientRequest> {

    private static final Codec objStreamCodec = new ObjectStreamCodec();

    private static final ByteCodeLoader classLoader = ByteCodeLoader.getInstance();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RawClientRequest msg) throws Exception {
        byte type = msg.getMessageType();
        ServerResponse resp;
        try {
            switch (type) {
                case CONNECT:
                    resp = handleConnect(msg);
                    break;
                case CREATE:
                    resp = handleCreate(msg);
                    break;
                case GET:
                    resp = handelGet(msg);
                    break;
                case GET_BY_FIELD:
                    resp = handleGetByField(msg);
                    break;
                case INSERT:
                    resp = handleInsert(msg);
                    break;
                case UPDATE:
                    resp = handleUpdate(msg);
                    break;
                case DELETE:
                    resp = handleDelete(msg);
                    break;
                case DROP:
                    resp = handleDrop(msg);
                    break;
                case BEAT:
                    resp = handleBeat(msg);
                    break;
                default:
                    throw new RuntimeException();
            }
        } catch (Exception e) {
            resp = ServerResponse.exception(msg.getSequenceId(), msg.getSerializeType(), "服务器处理请求时出现问题：\n" + ExceptionUtil.getStackTrace(e));
        }
        ctx.writeAndFlush(resp);
    }

    private ServerResponse handleConnect(RawClientRequest request) throws IOException {
        TableMap.getTable(request.connect().getTableName());
        return ServerResponse.connect(request.getSequenceId(), request.getSerializeType());
    }


    private ServerResponse handleCreate(RawClientRequest request) throws Exception {
        Codec tableCodec = BaseServerCodec.codecMap.get(request.getSerializeType());
        TableCreateParam param = (TableCreateParam) tableCodec.decodeBody(request.create().getRawTable(), TableCreateParam.class);
        byte[] rawIndexClass =request.create().getRawIndexClass();
        byte[] rawDataClass = request.create().getRawDataClass();
        Class<? extends Comparable<?>> indexClass =
                (Class<? extends Comparable<?>>) classLoader.loadClass(rawIndexClass);
        Class<?> dataClass =
                (Class<?>) classLoader.loadClass(rawDataClass);
        new Table<>(param.getTableName(), param.getDataSegmentSize(), param.getIndexSegmentSize(), param.getBlockSize(),
                indexClass, dataClass, rawIndexClass,rawDataClass,param.getMetaDataOffset());
        return ServerResponse.create(request.getSequenceId(), request.getSerializeType());
    }





    private ServerResponse handelGet(RawClientRequest request) throws Exception {
        Tree tree = TableMap.getTree(request.get().getTableName());
        Table table = TableMap.getTable(request.get().getTableName());
        Codec tableCodec = BaseServerCodec.codecMap.get(request.getSerializeType());
        byte[] data = tree.get(tableCodec.decodeBody(request.get().getRawIndex(), table.getIndexClass()));
        return ServerResponse.get(request.getSequenceId(), request.getSerializeType(), data);
    }

    private ServerResponse handleGetByField(RawClientRequest request) throws Exception {
        Tree tree = TableMap.getTree(request.get().getTableName());
        Table table = TableMap.getTable(request.get().getTableName());
        Codec dataCodec = BaseServerCodec.codecMap.get(table.getDataSerializeType());

        String fieldName = request.getByField().getFieldName();
        Field field = table.getDataClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        Comparable key = (Comparable) dataCodec.decodeBody(request.getByField().getRawKey(), field.getType());

        Iterator iterator = tree.iterator();
        Object res = null;
        int currentMaxSize = 1;
        int currentSize = 0;
        List<byte[]> results = new ArrayList<>(1);
        while (iterator.hasNext()) {
            byte[] data = (byte[]) iterator.next();
            Object dataObj = dataCodec.decodeBody(data, table.getDataClass());
            if (key.compareTo(field.get(dataObj)) == 0) {
                results.add(data);
            }
        }

        return ServerResponse.getByField(request.getSequenceId(),request.getSerializeType(),results);
    }

    private ServerResponse handleInsert(RawClientRequest request) throws Exception {
        Codec tableCodec = BaseServerCodec.codecMap.get(request.getSerializeType());
        Pair<Table<?>, Tree<?>> tableAndTree = TableMap.getTableAndTree(request.insert().getTableName());
        tableAndTree.getE2().add(
                (Comparable) tableCodec.decodeBody(request.insert().getRawIndex(), tableAndTree.getE1().getIndexClass())
                , request.insert().getData());
        return ServerResponse.insert(request.getSequenceId(), request.getSerializeType());
    }

    private ServerResponse handleUpdate(RawClientRequest request) throws Exception {
        Codec tableCodec = Objects.requireNonNull(BaseServerCodec.codecMap.get(request.getSerializeType()));
        TableMap.getTree(request.update().getTableName())
                .add(
                        (Comparable) tableCodec.decodeBody(request.update().getRawIndex(), Comparable.class)
                        , request.insert().getData()
                );
        return ServerResponse.update(request.getSequenceId(), request.getSerializeType());
    }

    private ServerResponse handleDelete(RawClientRequest request) throws Exception {
        Codec codec = Objects.requireNonNull(BaseServerCodec.codecMap.get(request.getSerializeType()));
        Table table = TableMap.getTable(request.get().getTableName());
        TableMap.getTree(request.delete().getTableName()).remove((Comparable) codec.decodeBody(request.delete().getRawIndex(), (Class<? extends Comparable>) table.getIndexClass()));
        return ServerResponse.delete(request.getSequenceId(), request.getSerializeType());
    }

    private ServerResponse handleDrop(RawClientRequest request) {
        return ServerResponse.drop(request.getSequenceId(), request.getSerializeType());
    }

    private ServerResponse handleBeat(RawClientRequest request) {
        log.debug("接收到心跳...");
        return ServerResponse.pong(request.getSequenceId(), request.getSerializeType());
    }

}
