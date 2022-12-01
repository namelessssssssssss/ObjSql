package com.objsql.session;


import com.objsql.db.Tree;
import com.objsql.db.entity.Pair;
import com.objsql.db.Table;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("all")
public class TableMap {

    /**
     * 活动的数据库实例
     */
    private static final Map<String, Pair<Table<?>, Tree<?>>> tables = new HashMap<>();


    public static Table<?> getTable(String tableName) throws IOException {
        Pair<Table<?>, Tree<?>> tableAndTree = tables.get(tableName);
        if (tableAndTree == null) {
            synchronized (TableMap.class) {
                if (tableAndTree == null) {
                    Table<?> table = Table.getInstance(tableName);
                    if (table == null) {
                        throw new RuntimeException("未找到指定的表");
                    }
                    tables.put(tableName,tableAndTree = new Pair<>(table, new Tree<>(table)));
                }
            }
        }
        return tableAndTree.getE1();
    }

    public static Tree<?> getTree(String tableName) {
        Tree<?> tree = tables.get(tableName).getE2();
        if (tree == null) {
            throw new RuntimeException("请先连接/创建对应的表");
        }
        return tree;
    }

    public static Pair<Table<?>, Tree<?>> getTableAndTree(String tableName){
        return tables.get(tableName);
    }

    public static void putTable(Table<?> table, Tree<?> tree) {
        tables.put(table.getTableName(), new Pair<>(table, tree));
    }

}
