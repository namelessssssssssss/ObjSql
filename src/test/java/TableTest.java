import com.alibaba.fastjson.JSON;

import com.objsql.db.Tree;
import com.objsql.db.Pair;
import com.objsql.db.Table;
import com.objsql.db.TableUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;


public class TableTest {

    @Test
    void test1() throws IOException {
        Table table = new Table("server", 2048, 1024, 5,Integer.class,String.class,4096);
        Tree<Integer> dataSource = new Tree<>(table);
        dataSource.add(1,"index = 1".getBytes());
        table.dropTable();
        return;
    }



    @Test
    void testSerialize(){
        Tree.Leaf leaf = new Tree.Leaf<>(5);
        leaf.id = 22;
        Pair<Integer,String> pair = new Pair<>();
        pair.setE1(1);
        pair.setE2("index=1");
        List list = new ArrayList<>();
        list.add(pair);
        leaf.indexedData = list;

        System.out.printf(JSON.toJSONString(leaf));
        System.out.printf(new String(leaf.serialize()));
    }

    @Test
    void testEof() throws IOException {
        RandomAccessFile file = new RandomAccessFile(new File("C:\\Users\\nameless\\Desktop\\B_plus_tree\\db\\server\\data"),"rw");
        System.out.println(new String(TableUtils.readToEnd(file.getChannel(),0,4056)));
    }

    /**
     * 顺序插入数据
     */
    @Test
    void testAddBlock() throws IOException {
        Table<Integer> table = new Table<>("test02", 2048, 1024, 5,Integer.class,String.class,4096);
        Tree<Integer> dataSource = new Tree<>(table);
        for(int l=0 ; l<100;l++) {
            dataSource.add(l, ("index = "+l).getBytes());
        }
    }

    /**
     * 乱序插入、读取数据
     */
    @Test
     void testAddDisorderly() throws IOException{
            Table<Long> table = new Table<>("test03", 2048, 1024, 5,Long.class,String.class,4096);
            List<Long> keys = new ArrayList<>();
            Tree<Long> dataSource = new Tree<>(table);
            for(int l=0; l<100;l++) {
                long key = (long) (Math.random() * l * 100);
                keys.add(key);
                dataSource.add(key, ("index = "+key).getBytes());
            }
            System.out.println("------------------------------");
            for(int l=0;l<100;l++) {
                System.out.println(new String(dataSource.get(keys.get(l))));
            }
            System.out.println("------------------------------");
            for(long k=0;k<10000;k++){
                try {
                    System.out.println(new String(dataSource.get(k)));
                }
                catch (NoSuchElementException ignored){}
            }
    }

    /**
     * 读取单个叶子节点
     */
    @Test
    void testReadLeaf() throws IOException {
        Table table = Table.getInstance("test02");
        Tree.Leaf leaf = table.readLeaf(0,5);
    }


    /**
     * 读取所有数据
     */
    @Test
    void testGet() throws IOException {
        Tree<Integer> tree = new Tree<>("test02");
        for(int i =99;i>=0;i--){
            System.out.println(new String(tree.get(i)));
        }
    }

    /**
     *  修改顺序数据为倒序
     */
    @Test
    void testUpdateDesc() throws IOException {
        Tree<Integer> tree = new Tree<>("test02");
        for(int l=0 ; l<100;l++) {
            tree.add(l, ("index = "+(99-l)).getBytes());
        }

    }

    /**
     * 修改倒序数据为正序
     */
    @Test
    void testUpdateAsc() throws IOException {
        Tree<Integer> tree = new Tree<>("test02");
        for(int l=0 ; l<100;l++) {
            tree.add(l, ("index = "+ l).getBytes());
        }
    }



}
