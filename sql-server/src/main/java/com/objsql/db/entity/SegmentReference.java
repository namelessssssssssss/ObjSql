package com.objsql.db.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;
import com.objsql.db.Tree;

import java.lang.ref.WeakReference;

/**
 * 叶子/数据节点缓存对象
 */
@JSONType(includes = "id")
public class SegmentReference {

    /**
     * 索引/数据段id
     */
    private final Integer id;

    /**
     * 索引或数据节点
     */
    @JSONField(serialize = false)
    private WeakReference<Segment> blockOrLeaf;

    private boolean isBlock(){
        return blockOrLeaf.get() instanceof Tree.Block;
    }

    public boolean hasInstance(){
        return blockOrLeaf.get()  != null;
    }

    public SegmentReference(Integer id){
        this.blockOrLeaf = new WeakReference<>(null);
        this.id = id;
    }

    public SegmentReference(Tree.Block<?> block){
        this.blockOrLeaf = new WeakReference<>(block);
        this.id = block.id;
    }

    public SegmentReference(Tree.Leaf<?> leaf){
        this.blockOrLeaf = new WeakReference<>(leaf);
        this.id = leaf.id;
    }

    public Integer getId(){
        return this.id;
    }

    public Segment getInstance(){
        return this.blockOrLeaf.get();
    }

    public void setInstance(Segment blockOrLeaf){
        this.blockOrLeaf = new WeakReference<>(blockOrLeaf);
    }
}
