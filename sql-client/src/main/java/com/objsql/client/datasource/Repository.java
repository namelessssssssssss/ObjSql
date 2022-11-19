package com.objsql.client.datasource;

import com.objsql.client.message.ClientRequest;
import com.objsql.client.message.HandledServerResponse;
import com.objsql.client.message.MissionQueue;
import com.objsql.common.message.TableCreateParam;
import com.objsql.common.util.Assert;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;


/**
 * 对某个表进行交互的数据层对象
 *
 * @param <Index>
 * @param <Data>
 */
@Slf4j
@lombok.Data
@Accessors(chain = true)
public class Repository<Index, Data> {

    private final String tableName;

    private final Class<Comparable<Index>> indexClass;

    private final Class<Data> dataClass;

    private final Byte serializeType;

    public Repository(String tableName, Class<Index> indexClass, Class<Data> dataClass) throws IllegalAccessException {
        Assert.isTrue(Comparable.class.isAssignableFrom(indexClass),"索引必须实现Comparable接口");
        this.tableName = tableName;
        this.indexClass = (Class<Comparable<Index>>) indexClass;
        this.dataClass = dataClass;
        HandledServerResponse response = connect();
        this.serializeType = response.getSerializeType();
    }

    public Repository(TableCreateParam<Index> param) {
        HandledServerResponse response = MissionQueue.submit(new ClientRequest().create().table(param).finish());
        this.tableName = param.getTableName();
        this.indexClass = param.getIndexClass();
        this.dataClass = (Class<Data>) param.getDataClass();
        this.serializeType = param.getSerializeType();
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

    private void create(TableCreateParam<Index> param){
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

    public void delete(Index index) {
        HandledServerResponse response = MissionQueue.submit(new ClientRequest().delete().tableName(tableName).key(index).finish());
        checkResponse(response, "删除");
    }
}
