
import com.objsql.db.Table;
import com.objsql.db.Tree;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class TreeTest {

    @Test
    void test() throws IOException {
        new Tree<>("BookRepository").printStucture();
        new Tree<>("LentRepository").printStucture();
    }


    //    //B+树随机、顺序插入 & 查找测试
//    @Test
//    public void add1() throws IOException {
//        long counter = System.currentTimeMillis();
//        Tree<Integer, String> bTree = new Tree<>(40);
//        for (int k = 0; k < 1000000; k++) {
//            int num = (int) (Math.random() * 10000000);
//            bTree.submitAsync(num, "index=" + num);
//        }
//        System.out.println("B+Tree:" + (counter = System.currentTimeMillis() - counter));
//        System.out.println(bTree.size());
//        int sizeCount=0;
//        for(String data:bTree){
//            sizeCount ++ ;
//        }
//        System.out.println(sizeCount);
//        //B+树查找100条数据
//        for (int c = 0; c < 100; c++) {
//            System.out.println(bTree.get(c));
//        }
//
//        //HashMap随机插入
//        long counter1 = System.currentTimeMillis();
//        Map<Integer, String> map = new HashMap<>();
//        for (int k = 0; k < 1000000; k++) {
//            int num = (int) (Math.random() * 10000000);
//            map.put(num, "index=" + num);
//        }
//        System.out.println("HashMap:" + (counter1 = System.currentTimeMillis() - counter1));
//
//
//        //列表随机插入后排序
//        long counter2 = System.currentTimeMillis();
//        List<String> list = new ArrayList<>();
//        for (int k = 0; k < 1000000; k++) {
//            int num = (int) (Math.random() * 10000000);
//            list.submitAsync("index=" + num);
//        }
//        Collections.sort(list);
//        System.out.println("ArrayList:" + (counter2 = System.currentTimeMillis() - counter2));
//
//        //B+树顺序插入
//        long counter3 = System.currentTimeMillis();
//        Tree<Integer, String> bpTree3 = new Tree<>(40);
//        for (int k = 0; k < 1000000; k++) {
//            bTree.submitAsync(k, "index=" + k);
//        }
//        System.out.println("B+Tree:" + (counter3 = System.currentTimeMillis() - counter3));
//
//
//        //列表顺序插入
//        long counter4 = System.currentTimeMillis();
//        List<String> list4 = new ArrayList<>();
//        for (int k = 0; k < 1000000; k++) {
//            list.submitAsync("index=" + k);
//        }
//        Collections.sort(list);
//        System.out.println("ArrayList:" + (counter4 = System.currentTimeMillis() - counter4));
//    }
//
    @Test
    public void add2() throws IOException {
        Table<Long> table = new Table<>("test03", 2048, 1024, 5, Long.class, String.class, 4096);
        Tree<Long> bTree = new Tree<>(table);
        List<Long> added = new ArrayList<>(1000);
        //添加1000个元素，可以重复
        for (long k = 0; k < 1000; k++) {
            long num = (int) (Math.random() * 1000);
            added.add(num);
            bTree.add(num, ("index=" + num).getBytes());
        }
        //测试查找添加的每一个元素
        for (Long index : added) {
            byte[] data;
            if ((data = bTree.get(index)).length > 0) {
                System.err.println("找不到元素" + index);
            } else {
                System.out.println("找到元素：" + new String(data));
            }
        }
//        //遍历底层叶子节点，测试其索引是否有序
//        for ( data : bTree) {
//            System.out.println(data);
//        }

    }

    @Test
    public void delete() throws IOException {
        Table<Long> table = new Table<>("test03", 2048, 1024, 5, Long.class, String.class, 4096);
        Tree<Long> tree = new Tree<>(table);
        for (long k = 0; k < 1000; k++) {
            tree.add(k, ("rawIndex=" + k).getBytes());
        }
        for (long k = 999; k >= 0; k--) {
            tree.remove(k);
        }

//        for (String data : tree) {
//            System.out.println(data);
//        }

        for (long k = 2000; k < 3000; k++) {
            tree.add(k, ("rawIndex=" + k).getBytes());
        }

//        for (String data : tree) {
//            System.out.println(data);
//        }
    }

}