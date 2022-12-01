package com.objsql.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.experimental.Accessors;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * B+树存储结构
 *
 * @author nameless
 */
@SuppressWarnings("all")
public class Tree<Index> implements Iterable<Byte[]> {


    /**
     * 该树的表文件
     */
    private final Table<Index> table;

    /**
     * 每块允许的最大元素数量
     */
    private final int blockSize;

    /**
     * 顶层索引
     */
    private Block<Index> top;
    /**
     * 叶子数据段头节点
     */
    private final Leaf<Index> head;

    /**
     * 目前所有叶子节点中元素的数量
     */
    private int size;

    /**
     * 通过表初始化树结构
     */
    public Tree(Table<Index> table) throws IOException {
        this.table = table;
        this.blockSize = table.getBlockSize();

        Leaf<Index> newLeaf = new Leaf<>(table.getBlockSize());
        newLeaf.id = table.addLeaf(newLeaf);

        this.head = newLeaf;
        Block<Index> newBlock = new Block<>(table.getBlockSize());
        newBlock.children.add(
                new Pair<>(
                        newLeaf.id, null
                )
        );
        newBlock.childrenIsLeaf = true;
        newBlock.id = table.addBlock(newBlock);
        this.top = newBlock;
        table.updateTable();
    }

    /**
     * 通过已有的表初始化树结构
     *
     * @param tableName 表名
     */
    public Tree(String tableName) throws IOException {
        this.table = Table.getInstance(tableName);
        this.blockSize = table.getBlockSize();
        this.head = table.readLeaf(table.getDataHead(), this.blockSize);
        this.head.leafSize = blockSize;
        this.top = table.readBlock(table.getIndexHead(), this.blockSize);
        this.initialized = table.isInitialized();
    }


    /**
     * 获取一个叶子节点的迭代器，可用于非索引字段的查找及修改。
     */
    @Override
    public Iterator iterator() {
        return new LeafIterator();
    }


    /**
     * 索引段
     */
    @lombok.Data
    @Accessors(chain = true)
    public static class Block<Index> {

        /**
         * 获取当前索引段的索引，即子节点中排列最靠右，最大的索引(从小到大排列）
         */
        public Comparable<Index> getIndex() {
            return this.children.size() >= 1 ? (Comparable<Index>) this.children.get(this.children.size() - 1).getE2() : null;
        }

        /**
         * 所在物理段id
         */
        public int id;
        /**
         * 单个块最大的索引数
         */
        @JSONField(serialize = false)
        public int blockSize;
        /**
         * 子节点数据，包含一个Block或Leaf的索引，及其指向的物理页id。
         */
        public List<Pair<Integer, Index>> children;

        /**
         * 标记子节点id是否是数据节点
         */
        public boolean childrenIsLeaf;

        /**
         * 序列化
         */
        public byte[] serialize() {
            return JSON.toJSONBytes(this);
        }

        /**
         * 向该索引段中添加一个引用(索引或叶子)。
         * 1.若产生了分裂而未更新索引，db.Pair.index为null，db.Pair.data为分裂产生的新段。
         * 2.若仅更新索引，db.Pair.index为该段旧的索引及对应的新索引，db.Pair.Data为null，
         * 3.若产生了分裂，且更新了索引，db.Pair.index为该段旧的索引及对应的新索引，db.Pair.data为产生的新段
         * 4.若未产生分裂也未更新索引，返回null
         * 产生的新段将被添加到父节点之下。
         *
         * @param newBlockOrLeaf 要添加的索引
         */
        public UpdateElement add(Pair<Integer, Index> newBlockOrLeaf, Table<Index> table) throws IOException {
            Comparable<Index> index = (Comparable<Index>) newBlockOrLeaf.getE2();
            int place = 0;
            //标记新节点是否是最大的
            boolean isBiggest = true;
            for (; place < children.size(); place++) {
                //若要添加的节点索引小于当前遍历到的索引
                if (((Comparable) children.get(place).getE2()).compareTo((Index) index) > 0) {
                    isBiggest = false;
                    break;
                }
            }
            Pair<Integer, Comparable<Index>> newElement = new Pair<>();
            newElement.setE1(newBlockOrLeaf.getE1());
            newElement.setE2((Comparable<Index>) newBlockOrLeaf.getE2());
            //如果该索引大于所有索引，将其插入到队列尾部
            if (isBiggest) {
                children.add((Pair<Integer, Index>) newElement);
            }
            //将否则将新索引插入到指定位置之前
            else {
                children.add(place, (Pair<Integer, Index>) newElement);
            }
            //如果需要分裂，进行分裂操作
            if (this.children.size() == blockSize) {
                //创建新的块
                Block<Index> newBlock = new Block<>(blockSize);
                if (this.childrenIsLeaf) {
                    //若该索引节点的子节点是叶子节点，则其分裂产生的同层节点的子节点也是叶子节点
                    newBlock.childrenIsLeaf = true;
                }
                //重设每个块的索引
                newBlock.children = sublist(children, (blockSize / 2) + 1, blockSize - 1);
                //持久化新块...
                newBlock.id = table.addBlock(newBlock);
                children = sublist(children, 0, blockSize / 2);
                //返回原有段的更新后的索引，及新段的编号及索引
                return new UpdateElement(new Pair<>(this.id, this.getIndex()), new Pair<>(newBlock.id, newBlock.getIndex()));
            }//如果仅需父节点更新索引...
            else if (isBiggest) {
                //返回新索引
                return new UpdateElement(new Pair<>(this.id, this.getIndex()));
            }//如果未产生分裂，也不用更新索引...
            else {
                return null;
            }
        }

        /**
         * 将当前索引段的旧索引替换为新索引,不考虑新索引是否为最大
         *
         * @param old      旧索引
         * @param newIndex 新索引及其段号
         */
        public void replaceIndex(Comparable<Index> old, Pair<Integer, Comparable<Index>> newIndex) {

            for (Pair<Integer, Index> index : children) {
                if (((Comparable) index.getE2()).compareTo((Index) old) == 0) {
                    index.setE1(newIndex.getE1());
                    index.setE2((Index) newIndex.getE2());
                    return;
                }
            }
        }

        /**
         * 删除一个子节点
         *
         * @param place 子节点索引
         * @return 若需要父节点移除该索引，updateElement.needToDelete = true，若需父节点执行更新，updateElement.updateIndex != null
         */
        private UpdateElement delete(int place, Table<Index> table) {
            // 该节点要删除的索引位置是否是最后一个（该节点索引）
            boolean needRefresh = this.children.size() - 1 == place;
            table.removeIndex(this.children.get(place).getE1());
            this.children.remove(place);
            if (this.children.isEmpty()) {
                return new UpdateElement(true);
            } else if (needRefresh) {
                return new UpdateElement(false, new Pair<>(this.id, this.getIndex()));
            } else {
                return null;
            }
        }

        /**
         * 获取一个block或leaf的id
         *
         * @return 所在物理页id
         */
        private int getId(Object blockOrLeaf) {
            return (blockOrLeaf instanceof Block ? ((Block<Index>) blockOrLeaf).id : ((Leaf<Index>) blockOrLeaf).id);
        }


        public Block(int blockSize) {
            this.children = new ArrayList<>();
            this.blockSize = blockSize;
        }

        @JSONCreator
        public Block(int id, int blockSize, List<Pair<Integer, Index>> children, boolean childrenIsLeaf) {
            this.id = id;
            this.blockSize = blockSize;
            this.children = children;
            this.childrenIsLeaf = childrenIsLeaf;
        }

        @Override
        public String toString() {
            return "block.index=" + ( this.children != null && this.children.size() > 0  ? "" : this.children.get(this.children.size() - 1).getE2().toString());
        }

    }

    /**
     * 叶子数据段
     */
    @lombok.Data
    @Accessors(chain = true)
    public static class Leaf<Index> {
        /**
         * 获取该段的索引值
         */
        public Comparable<Index> getIndex() {
            return this.indexedData.size() >= 1 ? (Comparable<Index>) this.indexedData.get(this.indexedData.size() - 1).getE1() : null;
        }

        /**
         * 所在物理段编号
         */
        //    @JSONField(name = "i")
        public int id;
        /**
         * 叶子节点大小
         */
        @JSONField(serialize = false)
        public int leafSize;
        /**
         * 上一个数据段号
         */
        //  @JSONField(name = "p")
        public Integer prev;
        /**
         * 下一个数据段号
         */
        //  @JSONField(name = "n")
        public Integer next;
        /**
         * 单条数据与其对应的索引
         */
        //   @JSONField(name = "d")


        public List<Pair<Index, byte[]>> indexedData;

        public byte[] serialize() {
            return JSON.toJSONBytes(this);
        }

        /**
         * 在当前叶子数据段中添加一条数据。
         * 若未达到叶子大小上限，返回null，若达到上限，进行分裂并返回新叶子节点。
         * 这个新块将被添加到父索引节点之下。
         *
         * @param index 索引
         * @param data  数据
         * @return 若未达到叶子大小上限，返回null，若达到上限，返回新叶子节点。
         */
        public UpdateElement add(Index index, byte[] data, Table<Index> table, int blockSize) throws IOException {
            int place = 0;
            boolean isBiggest = true;
            boolean isReplace = false;
            int compareResult;
            for (; place < indexedData.size(); place++) {
                compareResult = ((Comparable) indexedData.get(place).getE1()).compareTo(index);
                //若要添加的节点索引小于当前遍历到的索引
                if (compareResult > 0) {
                    isBiggest = false;
                    break;
                } //若索引位置已有元素（更新数据）
                else if (compareResult == 0) {
                    isBiggest = false;
                    isReplace = true;
                    break;
                }
            }
            UpdateElement res = null;
            //如果新叶子的索引是最大的，将其插入到队尾
            if (isBiggest) {
                indexedData.add(new Pair(index, data));
            }
            //如果是更新数据
            else if (isReplace) {
                indexedData.get(place).setE2(data);
                //无须更新索引等信息
            }
            //非最大索引，且不是替换，则插入到指定位置
            else {
                indexedData.add(place, new Pair(index, data));
            }
            //如果需要分裂，进行分裂操作
            if (this.indexedData.size() == leafSize) {
                Leaf<Index> newLeaf = new Leaf<>(blockSize);
                //新页放在后边
                newLeaf.indexedData = sublist(this.indexedData, leafSize / 2 + 1, leafSize - 1);
                this.indexedData = sublist(this.indexedData, 0, leafSize / 2);
                //新页的索引为旧页原有最后的元素
                //更新旧页的索引为最后的元素
                newLeaf.next = this.next;
                newLeaf.prev = this.id;
                //创建新数据页并获取其id
                newLeaf.id = table.addLeaf(newLeaf);
                this.next = newLeaf.id;
                //返回该页新的索引信息及添加新页的索引信息
                res = new UpdateElement(new Pair<>(this.id, this.getIndex()), new Pair<>(newLeaf.id, newLeaf.getIndex()));
            } else {
                res = isBiggest ? new UpdateElement(new Pair<>(this.id, this.getIndex())) : null;
            }
            table.updateLeaf(this);
            return res;
        }

        /**
         * 删除一条数据
         *
         * @param index 索引
         * @return 若本叶子中已无数据，返回updateElement.needToDelete = true。否则返回null。
         * @throws NoSuchElementException 若找不到指定索引的数据，抛出该异常
         */
        public UpdateElement delete(Index index, Table<Index> table) throws IOException {
            int place = 0;
            //是否找到指定索引的数据
            boolean found = false;
            //找到的数据是否是最大的
            boolean isBiggest = false;
            for (; place < indexedData.size(); place++) {
                if (((Comparable) indexedData.get(place).getE1()).compareTo(index) == 0) {
                    if (place == indexedData.size() - 1) {
                        isBiggest = true;
                    }
                    found = true;
                    break;
                }
            }
            if (found) {
//                if (isBiggest && this.indexedData.size() > 1) {
//                    this.childRIndex = this.indexedData.get(place - 1).getE1();
//                }
                this.indexedData.remove(place);
                if (this.indexedData.isEmpty()) {
                    if (this.prev != null) {
                        Leaf<Index> prev = table.readLeaf(this.prev, this.leafSize);
                        prev.next = this.next;
                        table.updateLeaf(prev);
                    }
                    if (this.next != null) {
                        Leaf<Index> next = table.readLeaf(this.next, this.leafSize);
                        next.prev = this.prev;
                        table.updateLeaf(next);
                    }
                    table.removeData(this.id);
                    return new UpdateElement(true);
                }
                //若删除了最大的元素，且叶子中仍有数据，则父节点需要更新对该叶子的索引
                else if (isBiggest) {
                    table.updateLeaf(this);
                    return new UpdateElement(new Pair<>(this.id, this.getIndex()));
                }
                table.updateLeaf(this);
                return null;
            } else {
                throw new NoSuchElementException();
            }
        }

        public Leaf(int leafSize) {
            this.indexedData = new ArrayList<>(leafSize);
            this.leafSize = leafSize;
        }

        @JSONCreator
        private Leaf(int id, int leafSize, Integer prev, Integer next, List<Pair<Index, byte[]>> indexedData) {
            this.id = id;
            this.leafSize = leafSize;
            this.prev = prev;
            this.next = next;
            this.indexedData = indexedData;
        }

        @Override
        public String toString() {
            return "leaf.index=" + ( this.indexedData != null && this.indexedData.size() > 0 ? this.indexedData.get(this.indexedData.size() - 1).getE1().toString() : "");
        }
    }

    private boolean initialized = false;

    /**
     * 添加一个元素。索引必须实现Comparable接口。
     */
    public void add(Comparable<Index> index, byte[] data) throws IOException {
        if (index == null) {
            throw new RuntimeException("索引不能为空");
        }
        if (!initialized) {
            //构造函数创建时，构造了一个空的Leaf节点。第一次插入时手动为该Leaf节点放入数据。
            top = new Block<Index>(table.getBlockSize()).setId(0).setChildrenIsLeaf(true);
            top.getChildren().add(new Pair(0, index));
            table.updateBlock(top);
            Leaf<Index> leaf = new Leaf<>(table.getBlockSize());
            leaf.setId(0);
            leaf.add((Index) index, data, table, blockSize);
            table.updateLeaf(leaf);
            initialized = true;
            table.setInitialized(true);
            table.updateTable();
            this.size++;
        } else {
            //如果Block的子节点类型为Leaf，表示已找到子节点
            UpdateElement newBlock = findAndUpdate((Index) index, data, this.top);
            //如果top的子节点产生分裂/需要添加/修改索引
            if (newBlock != null) {
                //若top节点产生分裂
                if (newBlock.newIndex != null) {
                    //创建新的top节点
                    Block<Index> newTop = new Block<>(blockSize);
                    //将原top节点和新产生的节点添加进去
                    newTop.add(new Pair(top.id, top.getIndex()), table);
                    newTop.add(new Pair(newBlock.newIndex.getE1(), newBlock.newIndex.getE2()), table);
                    this.top = newTop;
                    newTop.id = table.addBlock(newTop);
                    //设定新的头节点
                    table.setIndexHead(newTop.id);
                    table.updateTable();
                }
                //top已是最顶层节点
                else if (newBlock.updateIndex != null) {
//                    this.top.replaceIndex(newBlock.updateIndex.getE2(), newBlock.newIndex);
                }
            }
        }
    }

    /**
     * 删除指定索引位置的元素
     *
     * @param index 索引
     * @return 是否删除成功。若不存在指定的元素，返回false。
     * @throws NoSuchElementException 若没有找到指定索引的元素，抛出该异常
     */
    public boolean remove(Comparable<Index> index) throws NoSuchElementException, IOException {
        UpdateElement updateElement = findAndUpdate((Index) index, null, this.top);
        if (updateElement != null && updateElement.needDelete) {
            //表示根节点已空(表已空),下次添加元素时执行初始化逻辑
            //    top.children.submitAsync(new Pair<>(head.id, head.getIndex()));
            this.initialized = false;
            table.setIndexHead(0);
            table.setDataHead(0);
            table.getAvailableDataSegmentSlot().clear();
            table.getAvailableIndexSegmentSlot().clear();
            table.setInitialized(false);
            table.updateTable();
        }
        //根节点没有父节点，无须再更新
        return true;
    }

    public int size() {
        return this.size;
    }

    /**
     * 查找一个元素
     *
     * @param index 该元素的索引
     * @return 要查找的元素。查找失败，返回null
     */
    public byte[] get(Index index) throws IOException {
        if(!initialized){
            synchronized (this) {
                if(!initialized) {
                    return null;
                }
            }
        }
        try {
            Leaf<Index> leaf = findLeaf(index);
            for (Pair<Index, byte[]> data : leaf.indexedData) {
                if ((toComparable((Index) data.getE1()).compareTo(index) == 0)) {
                    return data.getE2();
                }
            }
        } catch (NoSuchElementException e) {
            return null;
        }
        return null;
    }


    /**
     * 将index转换为comparable
     */
    @SuppressWarnings("unchecked")
    private Comparable<Index> toComparable(Index index) {
        return (Comparable<Index>) index;
    }

    /**
     * 将block转换为带泛型的对象
     */
    @SuppressWarnings("unchecked")
    private Block<Index> toBlock(Object block) {
        return (Block<Index>) block;
    }

    /**
     * 将leaf转换为带泛型的对象
     */
    @SuppressWarnings("unchecked")
    private Leaf<Index> toLeaf(Object leaf) {
        return (Leaf<Index>) leaf;
    }

    /**
     * 递归查找并添加/删除元素，同时处理页分裂/合并
     *
     * @param blockOrLeaf 查找开始的节点
     * @param index       索引
     * @param data        数据。若是删除操作为null。
     */
    private UpdateElement findAndUpdate(Index index, byte[] data, Object blockOrLeaf) throws IOException {
        if (blockOrLeaf instanceof Leaf) {
            UpdateElement res = null;
            if (data != null) {
                //增加
                res = toLeaf(blockOrLeaf).add(index, data, table, blockSize);
                if (res == null) {
                    return null;
                } else {
                    this.size++;
                    return res;
                }
            } else {
                //删除
                res = toLeaf(blockOrLeaf).delete(index, table);
                this.size--;
                return res;
            }
        } else {
            int place = 0;
            for (; place < toBlock(blockOrLeaf).children.size(); place++) {
                //如果是插入数据
                if (data != null) {
                    //由于每个Block的索引是其子节点中最大的索引，第一个索引比查找索引大的Block就是要继续查找的Block。
                    //若其比所有Block的索引都大，就访问最后一个Block。
                    if (toComparable(index).compareTo((Index) (toBlock(blockOrLeaf)).children.get(place).getE2()) <= 0
                            || place == (toBlock(blockOrLeaf).children.size() - 1)) {
                        int childId = toBlock(blockOrLeaf).children.get(place).getE1();
                        Object next = toBlock(blockOrLeaf).childrenIsLeaf ? table.readLeaf(childId, this.blockSize) : table.readBlock(childId, this.blockSize);
                        UpdateElement needToAdd = findAndUpdate(index, data, next);
                        UpdateElement res = null;
                        //若返回了UpdateElement，表示该层索引需要添加/更新索引
                        if (needToAdd != null) {
                            //如果子块更新了索引...
                            if (needToAdd.updateIndex != null) {
                                //子块更新的新索引
                                Comparable<Index> newIndex = (Comparable<Index>) needToAdd.updateIndex.getE2();
                                //替换子块的旧索引为新索引
                                toBlock(blockOrLeaf).children.get(place).setE2((Index) newIndex);
                                //如果修改的是最后一个元素（索引元素），该块的父块也需要更新索引
                                if (place == toBlock(blockOrLeaf).children.size() - 1) {
                                    res = new UpdateElement(new Pair<>(null, newIndex));
                                }
//                                ((Block) blockOrLeaf).replaceIndex(
//                                        //替换本块未更新的旧索引
//                                        ((Pair<Integer,Comparable<Index>>)((Block) blockOrLeaf).children.get(place)).getE2(),
//                                        needToAdd.updateIndex
//                                );
                            }
                            //如果返回了新Block或Leaf
                            if (needToAdd.newIndex != null) {
                                res = (toBlock(blockOrLeaf).add((Pair<Integer, Index>) needToAdd.newIndex, table));
                            }
                            //统一更新当前索引页信息
                            table.updateBlock(toBlock(blockOrLeaf));
                        }
                        //没有产生分裂和索引更新
                        return res;
                    }
                }
                //如果是删除数据
                else {
                    UpdateElement result;
                    UpdateElement re = null;
                    if (toComparable(index).compareTo((Index) (toBlock(blockOrLeaf).children.get(place)).getE2()) <= 0
                            || place == toBlock(blockOrLeaf).children.size() - 1) {
                        int childId = toBlock(blockOrLeaf).children.get(place).getE1();
                        Object next = toBlock(blockOrLeaf).childrenIsLeaf ? table.readLeaf(childId, this.blockSize) : table.readBlock(childId, this.blockSize);
                        result = findAndUpdate(index, null, next);
                        if (result == null) {
                            re = null;
                        } else if (result.needDelete) {
                            //需要删除子节点
                            re = toBlock(blockOrLeaf).delete(place, table);
                        } else if (result.updateIndex != null) {
                            toBlock(blockOrLeaf).children.get(place).setE2((Index) result.updateIndex.getE2());
                            //如果更新的是最后一个索引，则父节点需要更新该节点的索引，传回该节点的新索引
                            if (place == toBlock(blockOrLeaf).children.size() - 1) {
                                re = new UpdateElement(new Pair<>(null, toBlock(blockOrLeaf).getIndex()));
                            }

                        }
                        table.updateBlock(toBlock(blockOrLeaf));
                        return re;
                    }
                }
            }
        }
        //正常情况下，应在之前的循环返回。
        throw new IllegalStateException();
    }

    /**
     * 用于递归中传递添加/删除元素信息的参数类
     */
    private static class UpdateElement {
        /**
         * 如果当前是进行删除操作，标记子节点是否可以删除
         */
        public boolean needDelete = false;
        /**
         * 旧索引要修改为的新索引,<新索引物理段id，新索引值>
         */
        public Pair<Integer, Comparable<?>> newIndex;
        /**
         * 需要更新的旧索引,<索引物理段id，更新后的新索引值>
         */
        public Pair<Integer, Comparable<?>> updateIndex;


        public UpdateElement() {
        }

        public UpdateElement(boolean needDelete, Pair<Integer, Comparable<?>> newIndex) {
            this.needDelete = needDelete;
            this.newIndex = newIndex;
        }

        public UpdateElement(boolean needDelete) {
            this.needDelete = needDelete;
        }

        public UpdateElement(Pair<Integer, Comparable<?>> updateIndex, Pair<Integer, Comparable<?>> newIndex) {
            this.updateIndex = updateIndex;
            this.newIndex = newIndex;
        }

        public UpdateElement(Pair<Integer, Comparable<?>> updateIndex) {
            this.updateIndex = updateIndex;
        }

        public UpdateElement(boolean needDelete, Pair<Integer, Comparable<?>> updateIndex, Pair<Integer, Comparable<?>> newIndex) {
            this.needDelete = needDelete;
            this.newIndex = newIndex;
            this.updateIndex = updateIndex;
        }
    }


    private Comparable<?> getIndex(Object blockOrLeaf) {
        return (blockOrLeaf instanceof Block ? ((Block) blockOrLeaf).getIndex() : ((Leaf) blockOrLeaf).getIndex());
    }


    /**
     * 以凹入表形式打印树结构
     */
    public void printStucture() throws IOException {
        print(this.top, 0);
    }

    private void print(Object now, int depth) throws IOException {
        System.out.println(getGap(depth) + now.toString());
        if (now instanceof Leaf || now == null) {
            for (Object pair : ((Leaf) now).getIndexedData()) {
                System.out.println(getGap(depth + 1) + "data.index =" + ((Pair) pair).getE1());
            }
            return;
        }
        List<Pair> blockPairs = ((Block) now).children;
        for (Pair p : blockPairs) {
            if (!((Block<?>) now).childrenIsLeaf) {
                print(table.readBlock((Integer) p.getE1(), table.getBlockSize()), depth + 1);
            } else {
                print(table.readLeaf((Integer) p.getE1(), table.getBlockSize()), depth + 1);
            }
        }
    }

    private String getGap(int len) {
        StringBuilder builder = new StringBuilder("   ");
        while (len-- > 0) {
            builder.append("   ");
        }
        return builder.toString();
    }


    /**
     * 通过索引，循环查找其所在范围的叶子页
     *
     * @param index 索引
     * @return 可能所在的叶子页。子叶可以不包含该索引。
     */
    private Leaf<Index> findLeaf(Index index) throws IOException {
        Object now = this.top;
        //如果当前节点类型为Leaf，表示已找到叶子节点
        while (!(now instanceof Leaf)) {
            int place = 0;
            //找到index在当前索引列表中的范围
            for (Pair<Integer, Index> indexPair : ((Block<Index>) now).children) {
                if (((Comparable) index).compareTo(indexPair.getE2()) <= 0) {
                    break;
                }
                place++;
            }
            //进行下一层索引的查找
            if (place == ((Block<Index>) now).children.size()) {
                throw new NoSuchElementException();
            }
            now = readChild(now, place);
        }
        return (Leaf<Index>) now;
    }

    /**
     * 获取指定位置的子节点实例
     *
     * @param blockOrLeaf blockOrLeaf
     * @param place       位置
     * @return 读取的子节点，blockOrLeaf
     */
    private Object readChild(Object blockOrLeaf, int place) throws IOException {
        return ((Block<Index>) blockOrLeaf).childrenIsLeaf ?
                table.readLeaf(((Block<Index>) blockOrLeaf).children.get(place).getE1(), this.blockSize) :
                table.readBlock(((Block<Index>) blockOrLeaf).children.get(place).getE1(), this.blockSize);
    }

    /**
     * 截取原列表内元素，创建一个新列表
     *
     * @param base 原列表
     * @param from 开始截取的下标，包含该元素
     * @param to   截止的下标，包含该元素
     * @return 截取获得的新列表
     */
    private static <T> List<T> sublist(List<T> base, int from, int to) {
        List<T> newList = new ArrayList<>(to - from + 1);
        for (int k = from; k <= to; k++) {
            newList.add(base.get(k));
        }
        return newList;
    }


    /**
     * 叶子节点迭代器，可用于非索引字段的查找
     */
    private class LeafIterator implements Iterator<byte[]> {
        /**
         * 删除当前遍历到的叶子节点。
         */
        @Override
        public void remove() {
            try {
                Tree.this.remove(
                        (Comparable<Index>) ((Pair<Comparable<Index>, byte[]>) currentLeaf.indexedData.get(placeInCurrentLeaf)).getE1()
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Tree.this.size--;
        }

        /**
         * 当前所在的叶子段
         */
        private Leaf currentLeaf;

        /**
         * 当前所在叶子段中的位置
         */
        private int placeInCurrentLeaf;

        @Override
        public boolean hasNext() {
            return !((
                    (placeInCurrentLeaf == currentLeaf.indexedData.size())
                            && currentLeaf.next == null)
                    || !initialized
            );
        }

        /**
         * 获取下一个元素。若不存在，抛出NoSuchElementException。
         *
         * @return 数据
         */
        @Override
        public byte[] next() {
            if (hasNext()) {
                byte[] res = ((Pair<Comparable<Index>, byte[]>) currentLeaf.indexedData.get(placeInCurrentLeaf++)).getE2();
                if (placeInCurrentLeaf == currentLeaf.indexedData.size() && currentLeaf.next != null) {
                    try {
                        currentLeaf = Tree.this.table.readLeaf(currentLeaf.next, Tree.this.blockSize);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    placeInCurrentLeaf = 0;
                }
                return res;
            } else {
                throw new NoSuchElementException("没有更多的元素");
            }
        }

        public LeafIterator() {
            try {
                currentLeaf = table.readLeaf(table.getDataHead(), table.getBlockSize());
            } catch (IOException e) {
                e.printStackTrace(new PrintStream(System.err));
            }
            placeInCurrentLeaf = 0;
        }
    }


    @Override
    public String toString() {
        return "db.Tree{" +
                "top=" + top +
                ", head=" + head +
                '}';
    }

    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Tree<Index> tree = (Tree<Index>) o;
        return blockSize == tree.blockSize && initialized == tree.initialized && top.equals(tree.top) && head.equals(tree.head);
    }

}
