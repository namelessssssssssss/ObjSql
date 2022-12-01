package com.objsql.message;

import java.io.Serializable;

import static com.objsql.common.protocol.constants.MessageTypes.*;


/**
 * 初步解码的客户端请求消息
 */

public class RawClientRequest implements Serializable {
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

    private byte[] rawIndex;

    private byte[] data;

    private byte[] rawTable;

    private byte[] rawIndexClass;

    private byte[] rawDataClass;

    private String fieldName;

    private String tableName;

    int segmentId;

    int place;


    public Get get() {
        this.messageType = GET;
        return new Get();
    }

    public Connect connect() {
        this.messageType = CONNECT;
        return new Connect();
    }

    public Create create() {
        this.messageType = CREATE;
        return new Create();
    }

    public GetByField getByField() {
        this.messageType = GET_BY_FIELD;
        return new GetByField();
    }

    public Insert insert() {
        this.messageType = INSERT;
        return new Insert();
    }

    public Update update() {
        this.messageType = UPDATE;
        return new Update();
    }

    public Delete delete() {
        this.messageType = DELETE;
        return new Delete();
    }

    public Drop drop() {
        this.messageType = DROP;
        return new Drop();
    }

    public Ping ping() {
        this.messageType = BEAT;
        return new Ping();
    }


    public class Get {

        private Get() {
        }

        public Get tableName(String tableName) {
            RawClientRequest.this.tableName = tableName;
            return this;
        }

        public Get index(byte[] index) {
            RawClientRequest.this.rawIndex = index;
            return this;
        }

        public byte[] getRawIndex() {
            return RawClientRequest.this.rawIndex;
        }

        public String getTableName() {
            return RawClientRequest.this.tableName;
        }


        public RawClientRequest finish() {
            return RawClientRequest.this;
        }
    }

    public class Connect {

        private Connect() {
        }

        public Connect tableName(String tableName) {
            RawClientRequest.this.tableName = tableName;
            return this;
        }

        public String getTableName() {
            return RawClientRequest.this.tableName;
        }


        public RawClientRequest finish() {
            return RawClientRequest.this;
        }
    }

    public class Create {
        private Create() {
        }

        public Create table(byte[] rawTable) {
            RawClientRequest.this.rawTable = rawTable;
            return this;
        }

        public Create rawIndexClass(byte[] rawIndexClass) {
            RawClientRequest.this.rawIndexClass = rawIndexClass;
            return this;
        }

        public Create rawDataClass(byte[] rawDataClass) {
            RawClientRequest.this.rawDataClass = rawDataClass;
            return this;
        }

        public byte[] getRawTable() {
            return RawClientRequest.this.rawTable;
        }

        public byte[] getRawIndexClass() {
            return RawClientRequest.this.rawIndexClass;
        }

        public byte[] getRawDataClass() {
            return RawClientRequest.this.rawDataClass;
        }

        public RawClientRequest finish() {
            return RawClientRequest.this;
        }
    }

    public class GetByField {

        private GetByField() {
        }

        public GetByField tableName(String tableName) {
            RawClientRequest.this.tableName = tableName;
            return this;
        }

        public GetByField fieldName(String fieldName) {
            RawClientRequest.this.fieldName = fieldName;
            return this;
        }

        public GetByField rawKey(byte[] rawKey){
            RawClientRequest.this.rawIndex = rawKey;
            return this;
        }

        public String getTableName() {
            return RawClientRequest.this.tableName;
        }

        public int getSegmentId() {
            return RawClientRequest.this.segmentId;
        }

        public int getPlace() {
            return RawClientRequest.this.place;
        }

        public String getFieldName() {
            return RawClientRequest.this.fieldName;
        }
        public byte[] getRawKey(){
            return RawClientRequest.this.rawIndex;
        }

        public RawClientRequest finish() {
            return RawClientRequest.this;
        }
    }

    public class Insert {

        private Insert() {
        }

        public Insert tableName(String tableName) {
            RawClientRequest.this.tableName = tableName;
            return this;
        }

        public Insert rawIndex(byte[] rawIndex) {
            RawClientRequest.this.rawIndex = rawIndex;
            return this;
        }

        public Insert data(byte[] data) {
            RawClientRequest.this.data = data;
            return this;
        }

        public String getTableName() {
            return RawClientRequest.this.tableName;
        }

        public byte[] getRawIndex() {
            return RawClientRequest.this.rawIndex;
        }

        public byte[] getData() {
            return RawClientRequest.this.data;
        }

        public RawClientRequest finish() {
            return RawClientRequest.this;
        }
    }

    public class Delete {

        private Delete() {
        }

        public Delete tableName(String tableName) {
            RawClientRequest.this.tableName = tableName;
            return this;
        }

        public Delete rawIndex(byte[] rawIndex) {
            RawClientRequest.this.rawIndex = rawIndex;
            return this;
        }

        public String getTableName() {
            return RawClientRequest.this.tableName;
        }

        public byte[] getRawIndex() {
            return RawClientRequest.this.rawIndex;
        }

        public RawClientRequest finish() {
            return RawClientRequest.this;
        }
    }

    public class Update {

        private Update() {
        }

        public Update tableName(String tableName) {
            RawClientRequest.this.tableName = tableName;
            return this;
        }

        public Update rawIndex(byte[] rawIndex) {
            RawClientRequest.this.rawIndex = rawIndex;
            return this;
        }

        public Update data(byte[] data) {
            RawClientRequest.this.data = data;
            return this;
        }

        public String getTableName() {
            return RawClientRequest.this.tableName;
        }

        public byte[] getRawIndex() {
            return RawClientRequest.this.rawIndex;
        }

        public byte[] getData() {
            return RawClientRequest.this.data;
        }

        public RawClientRequest finish() {
            return RawClientRequest.this;
        }

    }

    public class Drop {
        private Drop() {
        }

        public Drop tableName(String tableName) {
            RawClientRequest.this.tableName = tableName;
            return this;
        }


        public String getTableName() {
            return RawClientRequest.this.tableName;
        }

        public RawClientRequest finish() {
            return RawClientRequest.this;
        }
    }

    public class Ping {
        public RawClientRequest finish() {
            return RawClientRequest.this;
        }
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

    public RawClientRequest serializeType(byte type) {
        this.serializeType = type;
        return this;
    }

    public RawClientRequest() {
    }

    public int getPlace() {
        return place;
    }

    public void setSequenceId(int sequenceId) {
        this.sequenceId = sequenceId;
    }
}
