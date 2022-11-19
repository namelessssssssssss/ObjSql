package com.objsql.handler;

import com.objsql.common.message.TableCreateParam;
import com.objsql.common.util.ExceptionUtil;
import com.objsql.db.Tree;
import com.objsql.db.Pair;
import com.objsql.db.Table;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import com.objsql.codec.BaseServerCodec;
import com.objsql.common.codec.Codec;
import com.objsql.common.codec.ObjectStreamCodec;
import com.objsql.message.RawClientRequest;
import com.objsql.message.ServerResponse;
import com.objsql.session.TableMap;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Objects;

import static com.objsql.common.protocol.constants.MessageTypes.*;


@SuppressWarnings("all")
@Slf4j
@ChannelHandler.Sharable
public class ClientRequestHandler extends SimpleChannelInboundHandler<RawClientRequest> {

    private static final Codec objStreamCodec = new ObjectStreamCodec();

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
                case GET_BY_SEG_ID:
                    resp = handleGetBySeg(msg);
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
                default: throw new RuntimeException();
            }
        } catch (Exception e) {
            resp = ServerResponse.exception(msg.getSequenceId(), msg.getSerializeType(), "服务器处理请求时出现问题：\n" + ExceptionUtil.getStackTrace(e));
        }
        ctx.writeAndFlush(resp);
    }

    private ServerResponse handleConnect(RawClientRequest request) throws IOException {
        TableMap.getTable(request.connect().getTableName());
        return ServerResponse.connect(request.getSequenceId(),request.getSerializeType());
    }


    private ServerResponse handleCreate(RawClientRequest request) throws Exception {
        Codec tableCodec = BaseServerCodec.codecMap.get(request.getSerializeType());
        Class<? extends Comparable<?>> indexClass = (Class<? extends Comparable<?>>) objStreamCodec.decodeBody(request.create().getRawIndexClass(), Class.class);
        TableCreateParam param = (TableCreateParam) tableCodec.decodeBody(request.create().getRawTable(), TableCreateParam.class);
        new Table<>(param.getTableName(), param.getDataSegmentSize(), param.getIndexSegmentSize(), param.getBlockSize(), indexClass, param.getMetaDataOffset());
        return ServerResponse.create(request.getSequenceId(),request.getSerializeType());
    }

    private ServerResponse handelGet(RawClientRequest request) throws Exception {
        Tree tree = TableMap.getTree(request.get().getTableName());
        Table table = TableMap.getTable(request.get().getTableName());
        Codec tableCodec = BaseServerCodec.codecMap.get(request.getSerializeType());
        byte[] data = tree.get(tableCodec.decodeBody(request.get().getRawIndex(), table.getIndexClass()));
        return ServerResponse.get(request.getSequenceId(), request.getSerializeType(),data);
    }

    private ServerResponse handleGetBySeg(RawClientRequest request) {
        return null;
    }

    private ServerResponse handleInsert(RawClientRequest request) throws Exception {
        Codec tableCodec = BaseServerCodec.codecMap.get(request.getSerializeType());
        Pair<Table<?>, Tree<?>> tableAndTree = TableMap.getTableAndTree(request.insert().getTableName());
        tableAndTree.getE2().add(
                (Comparable) tableCodec.decodeBody(request.insert().getRawIndex(),tableAndTree.getE1().getIndexClass())
                , request.insert().getData());
        return ServerResponse.insert(request.getSequenceId(),request.getSerializeType());
    }

    private ServerResponse handleUpdate(RawClientRequest request) throws Exception {
        Codec tableCodec = Objects.requireNonNull(BaseServerCodec.codecMap.get(request.getSerializeType()));
        TableMap.getTree(request.update().getTableName())
                .add(
                        (Comparable)tableCodec.decodeBody(request.update().getRawIndex(), Comparable.class)
                        , request.insert().getData()
                );
        return ServerResponse.update(request.getSequenceId(),request.getSerializeType());
    }

    private ServerResponse handleDelete(RawClientRequest request) throws Exception {
        Codec codec = Objects.requireNonNull(BaseServerCodec.codecMap.get(request.getSerializeType()));
        Table table = TableMap.getTable(request.get().getTableName());
        TableMap.getTree(request.delete().getTableName()).remove((Comparable)codec.decodeBody(request.delete().getRawIndex(), (Class<? extends Comparable>) table.getIndexClass()));
        return ServerResponse.delete(request.getSequenceId(),request.getSerializeType());
    }

    private ServerResponse handleDrop(RawClientRequest request) {
        return ServerResponse.drop(request.getSequenceId(),request.getSerializeType());
    }

    private ServerResponse handleBeat(RawClientRequest request){
        log.debug("接收到心跳...");
        return ServerResponse.pong(request.getSequenceId(), request.getSerializeType());
    }

}
