package com.objsql.common.message;

import lombok.experimental.Accessors;

import java.io.Serializable;

@lombok.Data
@Accessors(chain = true)
public class TableCreateParam<Index,Data> implements Serializable {
    private Class<? extends Comparable<Index>> indexClass;

    private Class<Data> dataClass;

    private String tableName;

    private int dataSegmentSize;

    private int indexSegmentSize;

    private int blockSize;

    private int metaDataOffset;

    private byte indexSerializeType;

    private byte dataSerializeType;

    public TableCreateParam(Class<? extends Comparable<Index>> indexClass, Class<Data> dataClass, String tableName, int dataSegmentSize, int indexSegmentSize, int blockSize, int metaDataOffset) {
        this.indexClass =  indexClass;
        this.dataClass = dataClass;
        this.tableName = tableName;
        this.dataSegmentSize = dataSegmentSize;
        this.indexSegmentSize = indexSegmentSize;
        this.blockSize = blockSize;
        this.metaDataOffset = metaDataOffset;
    }

    public TableCreateParam(Class<? extends Comparable<Index>> indexClass, Class<Data> dataClass, String tableName, int dataSegmentSize, int indexSegmentSize, int blockSize, int metaDataOffset, byte indexSerializeType, byte dataSerializeType) {
        this.indexClass =indexClass;
        this.dataClass = dataClass;
        this.tableName = tableName;
        this.dataSegmentSize = dataSegmentSize;
        this.indexSegmentSize = indexSegmentSize;
        this.blockSize = blockSize;
        this.metaDataOffset = metaDataOffset;
        this.indexSerializeType = indexSerializeType;
        this.dataSerializeType = dataSerializeType;
    }
}
