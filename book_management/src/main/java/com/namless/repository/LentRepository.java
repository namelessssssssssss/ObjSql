package com.namless.repository;

import com.namless.entity.po.Lent;
import com.objsql.client.datasource.BaseRepository;
import com.objsql.common.message.TableCreateParam;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class LentRepository extends BaseRepository<Integer, Lent> {

    public LentRepository() throws IllegalAccessException {
        super(LentRepository.class.getSimpleName(), Integer.class, Lent.class);
    }

    public LentRepository(int dataSegmentSize, int indexSegmentSize, int blockSize, int metadataOffset) {
        super(new TableCreateParam<>(Integer.class, Lent.class, LentRepository.class.getSimpleName(), dataSegmentSize, indexSegmentSize, blockSize, metadataOffset));
    }

    /**
     * 添加一条记录
     */
    public void add(Lent lent) {
        super.add(lent.getId(), lent);
    }

    public Lent get(int id) {
        return super.get(id);
    }

    public List<Lent> getByDocCode(int docCode) {
        try {
            return super.getByField(docCode, "docCode");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<Lent> getByBorrowerName(String name) {
        try {
            return super.getByField(name, "borrowerName");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除一条记录
     *
     * @param id 记录id
     */
    public void delete(int id) {
        super.delete(id);
    }

}
