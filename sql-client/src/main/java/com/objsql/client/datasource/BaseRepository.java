package com.objsql.client.datasource;

import com.objsql.client.ClientBoot;
import com.objsql.client.message.ClientRequest;
import com.objsql.client.message.HandledServerResponse;
import com.objsql.client.message.MissionQueue;
import com.objsql.common.message.TableCreateParam;
import com.objsql.common.util.common.Assert;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.List;
import java.util.function.Function;


/**
 * 对某个表进行交互的数据层对象
 *
 * @param <Index>
 * @param <Data>
 */
@Slf4j
@lombok.Data
@Accessors(chain = true)
public class BaseRepository<Index, Data> {

    static {
        try {
            ClientBoot.boot();
            log.debug("尝试连接仓库服务器...");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private final String tableName;

    private final Class<Comparable<Index>> indexClass;

    private final Class<Data> dataClass;

    private final Byte serializeType;

    /**
     * 读取已有的表，创建仓库实例
     */
    public BaseRepository(String tableName, Class<Index> indexClass, Class<Data> dataClass) throws IllegalAccessException {
            Assert.isTrue(Comparable.class.isAssignableFrom(indexClass), "索引必须实现Comparable接口");
            this.tableName = tableName;
            this.indexClass = (Class<Comparable<Index>>) indexClass;
            this.dataClass = dataClass;
            HandledServerResponse response = connect();
            this.serializeType = response.getSerializeType();
    }

    /**
     * 创建新表，并创建仓库实例
     */
    public BaseRepository(TableCreateParam<Index,Data> param) {
        HandledServerResponse response = MissionQueue.submit(new ClientRequest().create().table(param).finish());
        this.tableName = param.getTableName();
        this.indexClass = (Class<Comparable<Index>>) param.getIndexClass();
        this.dataClass = param.getDataClass();
        this.serializeType = param.getIndexSerializeType();
        if (response != null && response.getErrorMessage() != null) {
            log.debug("已创建表:" + param.getTableName());
        }
        create(param);
        connect();
    }

    private void checkResponse(HandledServerResponse response, String operationType) {
        if (response != null && response.getErrorMessage() == null) {
            log.debug("对表" + tableName + "的操作[" + operationType + "]成功");
        } else {
            log.warn("对表" + tableName + "的操作[" + operationType + "]失败，原因：" + ((response == null) ? "未知原因" : response.getErrorMessage()));
        }
    }

    private void create(TableCreateParam<Index,Data> param){
        HandledServerResponse response =MissionQueue.submit(new ClientRequest().create().table(param).finish());
        checkResponse(response,"创建");
    }

    private HandledServerResponse connect() {
        HandledServerResponse response = MissionQueue.submit(new ClientRequest().connect().tableName(tableName).finish());
        checkResponse(response, "连接");
        return response;
    }

    public void add(Index index, Data data) {
        HandledServerResponse response = MissionQueue.submit(new ClientRequest().insert().tableName(tableName).key(index).data(data).finish());
        checkResponse(response, "添加");
    }

    public Data get(Index index) {
        HandledServerResponse response = MissionQueue.submit(new ClientRequest().get().tableName(tableName).index(index).dataClass(this.dataClass).finish());
        checkResponse(response, "查找");
        return (Data) response.getData();
    }

    public List<Data> getByField(Object key, Field field){
        HandledServerResponse response = MissionQueue.submit(new ClientRequest().getByField().tableName(tableName).dataClass(dataClass).field(field).key(key).finish());
        checkResponse(response,"非索引查找");
        return (List<Data>) response.getDataList();
    }

    public List<Data> getByField(Object key,String fieldName) throws NoSuchFieldException {
        HandledServerResponse response = MissionQueue.submit(new ClientRequest().getByField().tableName(tableName).dataClass(dataClass).field(dataClass.getDeclaredField(fieldName)).key(key).finish());
        checkResponse(response,"非索引查找");
        return (List<Data>) response.getDataList();
    }


    public void delete(Index index) {
        HandledServerResponse response = MissionQueue.submit(new ClientRequest().delete().tableName(tableName).key(index).finish());
        checkResponse(response, "删除");
    }

    public interface Getter<T,R> extends Function<T,R>{}
}
