package com.objsql.client.message;

import com.objsql.common.message.TableCreateParam;

import java.io.Serializable;
import java.lang.reflect.Field;

import static com.objsql.common.protocol.constants.MessageTypes.*;

/**
 * 客户端请求消息
 */

public class ClientRequest implements Serializable {
    /**
     * 请求序号
     */
    private int sequenceId;
    /**
     * 信息类型
     */
    private byte messageType;
    /**
     * 序列化方式
     */
    private byte serializeType;

    private Object index;

    private Object data;
    /**
     * 非索引字段查询的字段
     */
    private Field dataField;
    private Class<?> dataClass;

    private TableCreateParam tableParam;

    private String tableName;

    int segmentId;

    int place;

    /**
     * 发送该请求的线程
     */
    private Thread currentThread;


    public Get get() {
        this.currentThread = Thread.currentThread();
        this.messageType = GET;
        return new Get();
    }

    public Connect connect() {
        this.currentThread = Thread.currentThread();
        this.messageType = CONNECT;
        return new Connect();
    }

    public Create create() {
        this.currentThread = Thread.currentThread();
        this.messageType = CREATE;
        return new Create();
    }

    public GetByField getByField() {
        this.currentThread = Thread.currentThread();
        this.messageType = GET_BY_FIELD;
        return new GetByField();
    }

    public Insert insert() {
        this.currentThread = Thread.currentThread();
        this.messageType = INSERT;
        return new Insert();
    }

    public Update update() {
        this.currentThread = Thread.currentThread();
        this.messageType = UPDATE;
        return new Update();
    }

    public Delete delete() {
        this.currentThread = Thread.currentThread();
        this.messageType = DELETE;
        return new Delete();
    }

    public Drop drop() {
        this.currentThread = Thread.currentThread();
        this.messageType = DROP;
        return new Drop();
    }

    public Ping ping() {
        this.currentThread = Thread.currentThread();
        this.messageType = BEAT;
        return new Ping();
    }


    public class Get {

        public Get tableName(String tableName) {
            ClientRequest.this.tableName = tableName;
            return this;
        }

        public Get index(Object index) {
            ClientRequest.this.index = index;
            return this;
        }

        public Get dataClass(Class<?> dataClass) {
            ClientRequest.this.dataClass = dataClass;
            return this;
        }

        public Object getIndex() {
            return ClientRequest.this.index;
        }

        public String getTableName() {
            return ClientRequest.this.tableName;
        }

        public Class<?> getDataClass() {
            return ClientRequest.this.dataClass;
        }

        public ClientRequest finish() {
            return ClientRequest.this;
        }
    }

    public class Connect {

        public Connect tableName(String tableName) {
            ClientRequest.this.tableName = tableName;
            return this;
        }

        public ClientRequest finish() {
            return ClientRequest.this;
        }
    }

    public class Create {

        public Create table(TableCreateParam table) {
            ClientRequest.this.tableParam = table;
            return this;
        }

        public ClientRequest finish() {
            return ClientRequest.this;
        }
    }

    public class GetByField {
        public GetByField tableName(String tableName) {
            ClientRequest.this.tableName = tableName;
            return this;
        }

        public GetByField field(Field field) {
            ClientRequest.this.dataField = field;
            return this;
        }

        public GetByField key(Object key){
            ClientRequest.this.index = key;
            return this;
        }

        public GetByField dataClass(Class<?> clazz){
            ClientRequest.this.dataClass = clazz;
            return this;
        }

        public ClientRequest finish() {
            return ClientRequest.this;
        }
    }

    /**
     * 插入信息
     */
    public class Insert {
        public Insert tableName(String tableName) {
            ClientRequest.this.tableName = tableName;
            return this;
        }

        public Insert key(Object key) {
            ClientRequest.this.index = key;
            return this;
        }

        public Insert data(Object data) {
            ClientRequest.this.data = data;
            return this;
        }

        public ClientRequest finish() {
            return ClientRequest.this;
        }
    }

    /**
     * 删除信息
     */
    public class Delete {

        public Delete tableName(String tableName) {
            ClientRequest.this.tableName = tableName;
            return this;
        }

        public Delete key(Object key) {
            ClientRequest.this.index = key;
            return this;
        }

        public ClientRequest finish() {
            return ClientRequest.this;
        }
    }

    /**
     * 更新信息
     */
    public class Update {
        public Update tableName(String tableName) {
            ClientRequest.this.tableName = tableName;
            return this;
        }

        public Update key(Object key) {
            ClientRequest.this.index = key;
            return this;
        }

        public Update data(Object data) {
            ClientRequest.this.data = data;
            return this;
        }

        public ClientRequest finish() {
            return ClientRequest.this;
        }

    }

    /**
     * 删除表
     */
    public class Drop {
        public Drop tableName(String tableName) {
            ClientRequest.this.tableName = tableName;
            return this;
        }

        public ClientRequest finish() {
            return ClientRequest.this;
        }
    }

    /**
     * 心跳
     */
    public class Ping {
        public ClientRequest finish() {
            return ClientRequest.this;
        }
    }


    public ClientRequest() {
    }

    public int getSequenceId() {
        return sequenceId;
    }

    public byte getMessageType() {
        return messageType;
    }

    public byte getSerializeType() {
        return serializeType;
    }

    public void setSerializeType(byte serializeType) {
        this.serializeType = serializeType;
    }

    public Object getIndex() {
        return index;
    }

    public Object getData() {
        return data;
    }

    public TableCreateParam getTableParam() {
        return tableParam;
    }

    public String getTableName() {
        return tableName;
    }

    public int getSegmentId() {
        return segmentId;
    }

    public int getPlace() {
        return place;
    }

    public Thread getCurrentThread() {
        return currentThread;
    }

    public void setCurrentThread() {
        this.currentThread = Thread.currentThread();
    }

    public void setSequenceId(int sequenceId) {
        this.sequenceId = sequenceId;
    }

    public Field getDataField() {
        return dataField;
    }

    public Class<?> getDataClass() {
        return dataClass;
    }
}
