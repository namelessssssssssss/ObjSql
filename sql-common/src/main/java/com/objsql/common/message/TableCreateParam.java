package com.objsql.common.message;

import lombok.experimental.Accessors;

import java.io.Serializable;

@lombok.Data
@Accessors(chain = true)
public class TableCreateParam<Index> implements Serializable {
    private Class<Comparable<Index>> indexClass;

    private Class<?> dataClass;

    private String tableName;

    private int dataSegmentSize;

    private int indexSegmentSize;

    private int blockSize;

    private int metaDataOffset;

    private byte serializeType;

    public TableCreateParam(Class<? extends Comparable<Index>> indexClass, Class<?> dataClass, String tableName, int dataSegmentSize, int indexSegmentSize, int blockSize, int metaDataOffset) {
        this.indexClass = (Class<Comparable<Index>>) indexClass;
        this.dataClass = dataClass;
        this.tableName = tableName;
        this.dataSegmentSize = dataSegmentSize;
        this.indexSegmentSize = indexSegmentSize;
        this.blockSize = blockSize;
        this.metaDataOffset = metaDataOffset;
    }
}
