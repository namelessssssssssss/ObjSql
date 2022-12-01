package com.objsql.client.datasource;

import com.objsql.common.message.TableCreateParam;

import java.lang.reflect.Field;
import java.util.List;

@SuppressWarnings("all")
public abstract class AbstractRepository<Index, Data> {

    public BaseRepository<Index, Data> baseRepository;

    public String tableName;

    /**
     * 通过读取现有的表初始化仓库
     */
    public AbstractRepository() {
        tableName = tableName();
        try {
            baseRepository = new BaseRepository<>(tableName(), (Class<Index>) indexClass(), dataClass());
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 通过创建新表初始化仓库
     *
     * @param dataSegmentSize  数据段大小
     * @param indexSegmentSize 索引段大小
     * @param blockSize        树的阶数
     * @param metadataOffset   表头元数据大小
     */
    public AbstractRepository(int dataSegmentSize, int indexSegmentSize, int blockSize, int metadataOffset) {
        baseRepository = new BaseRepository<>(
                new TableCreateParam<>(indexClass(), dataClass(), "book", dataSegmentSize, indexSegmentSize, blockSize, metadataOffset)
        );
    }

    public Data getByIndex(Index index) {
        return baseRepository.get(index);
    }

    public List<Data> getByField(Object key, String fieldName) {
        try {
            return baseRepository.getByField(key, fieldName);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Data> getByField(Object key, Field field) {
        return baseRepository.getByField(key, field);
    }

    public abstract String tableName();

    public abstract Class<? extends Comparable<Index>> indexClass();

    public abstract Class<Data> dataClass();

}
