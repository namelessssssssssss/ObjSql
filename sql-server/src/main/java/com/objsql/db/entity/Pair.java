package com.objsql.db.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * 存储任意一对数据
 *
 * @author nameless
 */
@lombok.Data
@Accessors(chain = true)
@JSONType
public class Pair<E1, E2> implements Serializable {
    @JSONField
    private E1 e1;
    @JSONField
    private E2 e2;

    public E1 getE1() {
        return e1;
    }

    public E2 getE2() {
        return e2;
    }

    public Pair(E1 e1, E2 elementB) {
        this.e1 = e1;
        this.e2 = elementB;
    }

    public Pair(){};
    @Override
    public String toString() {
        return "db.Pair{" +
                "index=" + e1 +
                ", data=" + e2 +
                '}';
    }
}
