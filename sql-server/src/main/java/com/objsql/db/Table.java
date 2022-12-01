package com.objsql.db;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import com.objsql.common.codec.ObjectStreamCodec;
import com.objsql.common.util.protocol.ByteCodeLoader;
import com.objsql.common.util.protocol.ByteCodeWriter;
import com.objsql.common.util.common.ExceptionUtil;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 表文件
 * 描述表结构，以及其中数据
 *
 * @author nameless
 */
@lombok.Data
@Accessors(chain = true)
@Slf4j
@SuppressWarnings("all")
public class Table<Index> {
    @JSONField(serialize = false)
    private static ObjectStreamCodec objectStreamCodec = new ObjectStreamCodec();

    /**
     * 表名
     */
    @JSONField
    private String tableName;
    /**
     * 当前索引头节点的段号
     */
    @JSONField
    private int indexHead;
    /**
     * 当前数据头节点的段号
     */
    @JSONField
    private int dataHead;
    /**
     * 索引及叶子段中所含索引/数据条数
     */
    @JSONField
    private int blockSize;
    /**
     * 是否已初始化
     */
    @JSONField
    private boolean initialized;
    /**
     * 索引文件读写通道
     */
    @JSONField(serialize = false)
    private FileChannel indexChannel;
    /**
     * 数据文件读写通道
     */
    @JSONField(serialize = false)
    private FileChannel dataChannel;
    /**
     * 数据库路径,默认位于项目路径下
     */
    @JSONField(serialize = false)
    private static String publicFilePath;
    /**
     * 索引文件路径
     */
    @JSONField(serialize = false)
    private String indexPath;
    /**
     * 数据文件路径
     */
    @JSONField(serialize = false)
    private String dataPath;
    /**
     * 索引文件实例
     */
    @JSONField(serialize = false)
    private RandomAccessFile indexFile;
    /**
     * 索引文件名
     */
    @JSONField
    private static String indexFilePrefix = "index";
    /**
     * 数据节点文件实例
     */
    @JSONField(serialize = false)
    private RandomAccessFile dataFile;
    /**
     * 数据文件名
     */
    @JSONField
    private static String dataFilePrefix = "data";
    /**
     * 整个表的大小，包括总数据段大小 + 总索引段大小,byte
     */
    @JSONField
    private long tableSize;
    /**
     * 单个叶子数据段的大小，即单个叶子节点大小,byte
     */
    @JSONField
    private int dataSegmentSize;
    /**
     * 当前文件中包含叶子数据段总数
     */
    @JSONField
    private int dataSegmentCount;
    /**
     * 标记空闲的叶子数据段槽位编号
     */
    @JSONField
    private List<Integer> availableDataSegmentSlot;
    /**
     * 当前最大的索引段号
     */
    @JSONField
    private int currentIndexTail = 0;
    /**
     * 当前最大的数据段号
     */
    @JSONField
    private int currentDataTail = 0;
    /**
     * 单个索引数据段的大小,byte
     */
    @JSONField
    private int indexSegmentSize;
    /**
     * 当前文件中包含索引数据段总数
     */
    @JSONField
    private int indexSegmentCount;
    /**
     * 标记空闲的索引数据段槽位编号
     */
    @JSONField
    private List<Integer> availableIndexSegmentSlot;
    /**
     * 索引类Class
     */
    @JSONField(serialize = false)
    Class<Index> indexClass;
    /**
     * 数据类Class
     */
    @JSONField(serialize = false)
    Class dataClass;

    /**
     * 索引的序列化方式
     */
    private byte indexSerializeType;

    /**
     * 数据的序列化方式
     */
    private byte dataSerializeType;


    /**
     * 索引、数据Class序列化后所占长度
     */
    private int objStreamLength;

    /**
     * 表实例json序列化后所占长度
     */
    private int jsonTableSegmentLength;

    /**
     * 表文件头信息预留的偏移量，byte
     */
    private int metaDataOffset;

    /**
     * data文件结构：
     *  |len(index)|index(objStream)|len(data)|data(objStream)|len(jsonTableSegmentLentgh)|table(json)|  空余空间  | dataSegment0 | dataSegment1 | dataSegment2 |...
     *  |---------------------objStreamLength-----------------|-------------jsonTableSegmentLength---------------|
     *  |---------------------------------------------metaDataOffset---------------------------------------------|
     */

    static {
        Properties properties = new Properties();
        try {
            properties.load(Table.class.getResourceAsStream("/application.properties"));
        } catch (IOException e) {
            log.warn("读取配置文件时出现问题:{}", ExceptionUtil.getStackTrace(e));
            throw new RuntimeException(e);
        }
        publicFilePath = properties.getProperty("baseRepository.location");
        //   publicFilePath = "C:\\Users\\nameless\\Desktop\\B_plus_tree";
    }

    /**
     * 通过表名,从默认目录获取现有表实例
     *
     * @param tableName 表名
     * @return 表实例
     */
    public static Table getInstance(String tableName) throws IOException {
        //表头信息放在数据文件中
        File tableFile = new File(publicFilePath + File.separator + tableName + File.separator + dataFilePrefix);
        if (!tableFile.isFile()) {
            throw new RuntimeException("表名为" + tableName + "的表不存在");
        }
        try (FileChannel fileChannel = new RandomAccessFile(tableFile, "rw").getChannel()) {
            ByteBuffer intBuf = ByteBuffer.allocate(4);
            //读取索引类（ObjStream）
            fileChannel.read(intBuf);
            intBuf.flip();
            int indexClassObjLen = intBuf.getInt();
            intBuf.clear();
            ByteBuffer indexClassBuf = ByteBuffer.allocate(indexClassObjLen);
            fileChannel.read(indexClassBuf);
            indexClassBuf.flip();
            Class<Comparable<?>> index = (Class<Comparable<?>>) ByteCodeLoader.getInstance().loadClass(indexClassBuf.array());
            indexClassBuf.clear();

            //读取数据类（ObjStream）
            fileChannel.read(intBuf);
            intBuf.flip();
            int dataClassObjLen = intBuf.getInt();
            intBuf.clear();
            ByteBuffer dataClassBuf = ByteBuffer.allocate(dataClassObjLen);
            fileChannel.read(dataClassBuf);
            dataClassBuf.flip();
            Class<Comparable<?>> data = (Class<Comparable<?>>)ByteCodeLoader.getInstance().loadClass(dataClassBuf.array());
            dataClassBuf.clear();

            //读取表数据 （Json）
            fileChannel.read(intBuf);
            intBuf.flip();
            int jsonTableMaxLen = intBuf.getInt();
            Table table = JSON.parseObject(
                    TableUtils.readToEnd(fileChannel, indexClassObjLen + dataClassObjLen + (4 + 4) * 2 - 4, jsonTableMaxLen)
                    , Table.class
            );
            table.setIndexClass(index);
            table.setDataClass(data);
            return table;
        } catch (Exception e) {
            log.error("通过表名获取表实例时出现问题：\n" + ExceptionUtil.getStackTrace(e));
        }
        return null;
    }

    /**
     * 读取序列化的索引Class
     */
    private static Class<? extends Comparable<?>> readIndexClass(FileChannel channel) throws IOException, ClassNotFoundException {
        ByteBuffer intBuf = ByteBuffer.allocate(4);
        channel.read(intBuf);
        intBuf.flip();
        int len = intBuf.getInt();
        ByteBuffer objBuf = ByteBuffer.allocate(len);
        channel.read(objBuf);
        objBuf.flip();
        byte[] obj = new byte[len];
        objBuf.get(obj, 0, len);
        return (Class<? extends Comparable<?>>) objectStreamCodec.decodeBody(obj, Class.class);
    }

    @JSONCreator
    private Table(String tableName) {
        initFiles(tableName);
    }

    /**
     * 通过创建新表构造实例
     *
     * @param tableName        表名
     * @param indexSegmentSize 索引段大小
     * @param dataSegmentSize  数据段大小
     */
    public Table(String tableName, int dataSegmentSize, int indexSegmentSize, int blockSize, Class<Index> indexClass, Class dataClass, int metaDataOffset) throws IOException {
        this.blockSize = blockSize;
        this.dataPath = publicFilePath + File.separator + tableName + File.separator + "data";
        this.indexPath = publicFilePath + File.separator + tableName + File.separator + "index";
        this.tableName = tableName;
        File db = new File(publicFilePath);
        if (!db.isDirectory()) {
            if (new File(publicFilePath).mkdir()) {
                throw new RuntimeException("创建数据库目录失败");
            }
        }
        if (new File(publicFilePath + File.separator + tableName).mkdir()) {
            System.out.println("未找到指定名称的表。创建新表");
        }
        File dataFile = new File(dataPath);
        File indexFile = new File(indexPath);
        try {
            this.dataFile = new RandomAccessFile(dataFile, "rw");
            this.indexFile = new RandomAccessFile(indexFile, "rw");
        } catch (Exception e) {
            throw new RuntimeException("创建文件失败");
        }
        this.dataChannel = this.dataFile.getChannel();
        this.indexChannel = this.indexFile.getChannel();
        this.dataSegmentSize = dataSegmentSize;
        this.indexSegmentSize = indexSegmentSize;
        this.availableIndexSegmentSlot = new ArrayList<>();
        this.availableDataSegmentSlot = new ArrayList<>();
        this.indexHead = 1;
        this.indexClass = (Class<Index>) indexClass;
        this.dataClass = dataClass;
        this.metaDataOffset = metaDataOffset;
        writeHeader(dataChannel, indexClass, dataClass);
        this.updateTable();
    }

    private void initFiles(String tableName) {
        this.dataPath = publicFilePath + File.separator + tableName + File.separator + "data";
        this.indexPath = publicFilePath + File.separator + tableName + File.separator + "index";
        this.tableName = tableName;
        File dataFile = new File(dataPath);
        File indexFile = new File(indexPath);
        try {
            this.dataFile = new RandomAccessFile(dataFile, "rw");
            this.indexFile = new RandomAccessFile(indexFile, "rw");
        } catch (Exception e) {
            throw new RuntimeException("创建文件失败");
        }
        this.dataChannel = this.dataFile.getChannel();
        this.indexChannel = this.indexFile.getChannel();
    }


    /**
     * 写入文件头信息 (metaData)
     *
     * @param channel
     */
    private void writeHeader(FileChannel channel, Class<?> indexClass, Class<?> dataClass) {
        try {
            //写入indexClass
            ByteBuffer buf = ByteBuffer.wrap(
                    ByteCodeWriter.getClassBytes(indexClass)
            );
            ByteBuffer intBuf = ByteBuffer.allocate(4);
            this.objStreamLength = buf.capacity() + 4;
            intBuf.putInt(buf.capacity());
            intBuf.flip();
            channel.write(intBuf);
            channel.write(buf);

            //写入dataClass
            buf = ByteBuffer.wrap(
                    ByteCodeWriter.getClassBytes(dataClass)
            );
            intBuf = ByteBuffer.allocate(4);
            this.objStreamLength += buf.capacity() + 4;
            intBuf.putInt(buf.capacity());
            intBuf.flip();
            channel.write(intBuf);
            channel.write(buf);

            //写入表的其它信息（json）
            buf = ByteBuffer.wrap(JSON.toJSONBytes(this));
            intBuf.clear().putInt(buf.capacity());
            intBuf.flip();
            buf.flip();
            channel.write(intBuf);
            channel.write(buf);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("写入表头元数据时出现问题：\n" + ExceptionUtil.getStackTrace(e));
        }
    }


    /**
     * 更新表信息
     */
    public void updateTable() throws IOException {
        ByteBuffer newTableJson = ByteBuffer.wrap(JSON.toJSONBytes(this));
        ByteBuffer len = ByteBuffer.allocate(4);
        len.putInt(newTableJson.remaining());
        dataChannel.write(len, objStreamLength);
        dataChannel.write(newTableJson, objStreamLength + 4);
    }


    /**
     * 获取指定编号的索引段偏移量,byte
     *
     * @param index 索引段编号
     * @return 偏移量
     */
    private long getIndexSegmentOffset(int index) {
        return (long) index * indexSegmentSize;
    }

    /**
     * 获取指定编号的数据段偏移量,byte
     *
     * @param id 索引段编号
     * @return 偏移量
     */
    private long getDataSegmentOffset(int id) {
        return metaDataOffset + (long) id * dataSegmentSize;
    }


    /**
     * 获取一个可用的索引段编号
     */
    private int getNewIndexSegmentId() {
        return availableIndexSegmentSlot.size() > 1 ? availableIndexSegmentSlot.remove(availableIndexSegmentSlot.size() - 1) : currentIndexTail++;
    }

    /**
     * 获取一个可用的数据段编号
     */
    private int getNewDataId() {
        return availableDataSegmentSlot.size() > 1 ?
                availableDataSegmentSlot.remove(availableDataSegmentSlot.size() - 1)
                : currentDataTail++;
    }


    /**
     * 读取一个索引段
     *
     * @param index 该索引段的编号
     * @return Block
     */
    public Tree.Block<Index> readBlock(int index, int size) throws IOException {
        Tree.Block<Index> block = JSON.parseObject(
                new String(TableUtils.readToEnd(indexChannel, getIndexSegmentOffset(index), indexSegmentSize))
                , new TypeReference<>(indexClass) {
                }
        );
        return block.setBlockSize(size);
    }

    /**
     * 读取一个数据段
     *
     * @param index 该数据段的编号
     */
    public Tree.Leaf<Index> readLeaf(int index, int size) throws IOException {
        Tree.Leaf<Index> leaf = JSON.parseObject(
                new String(TableUtils.readToEnd(dataChannel, getDataSegmentOffset(index), dataSegmentSize))
                , new TypeReference<>(indexClass) {
                }
        );
        return leaf.setLeafSize(size);
    }


    /**
     * 存储一个新的索引段
     *
     * @param block 索引段
     */
    public int addBlock(Tree.Block<Index> block) throws IOException {
        ByteBuffer writeBuffer = ByteBuffer.allocate(indexSegmentSize);
        int id = getNewIndexSegmentId();
        block.setId(id);
        writeBuffer.put(block.serialize());
        writeBuffer.position(writeBuffer.capacity());
        writeBuffer.flip();
        indexChannel.write(writeBuffer, getIndexSegmentOffset(id));
        return id;
    }

    public int addLeaf(Tree.Leaf<Index> data) throws IOException {
        ByteBuffer writeBuffer = ByteBuffer.allocate(dataSegmentSize);
        int id = getNewDataId();
        data.setId(id);
        writeBuffer.put(data.serialize());
        writeBuffer.position(writeBuffer.capacity());
        writeBuffer.flip();
        dataChannel.write(writeBuffer, getDataSegmentOffset(id));
        return id;
    }

    public void updateBlock(Tree.Block<Index> block) throws IOException {
        ByteBuffer writeBuffer = ByteBuffer.allocate(indexSegmentSize);
        writeBuffer.put(block.serialize());
        writeBuffer.position(writeBuffer.capacity());
        writeBuffer.flip();
        indexChannel.write(writeBuffer, getIndexSegmentOffset(block.id));
    }

    public void updateLeaf(Tree.Leaf<Index> leaf) throws IOException {
        ByteBuffer writeBuffer = ByteBuffer.allocate(dataSegmentSize);
        writeBuffer.put(leaf.serialize());
        writeBuffer.position(writeBuffer.capacity());
        writeBuffer.flip();
        dataChannel.write(writeBuffer, getDataSegmentOffset(leaf.id));
    }

    /**
     * 移除一个索引段
     *
     * @param segmentId 索引段编号
     */
    public void removeIndex(int segmentId) {
        this.availableIndexSegmentSlot.add(segmentId);
    }

    public void removeData(int segmentId) {
        this.availableDataSegmentSlot.add(segmentId);
    }

    /**
     * 更新数据节点指针
     *
     * @param segmentId 节点id
     * @param next      下一个节点id
     */
    public void updateDataNextPointer(int segmentId, int next) {

    }

    /**
     * 更新数据节点指针
     *
     * @param segmentId 节点id
     * @param next      上一个节点id
     */
    public void updateDataPrevPointer(int segmentId, int next) {

    }

    /**
     * 更新某个索引段的子索引
     *
     * @param id      索引段编号
     * @param newData 新数据
     * @param place   新数据所在位置
     */
    public void replaceBlockIndex(int id, byte[] newData, int place) {
    }


    /**
     * 更新某个数据段的数据
     *
     * @param segmentIndex 段编号
     * @param newData
     * @return
     */
    public void replaceBlockData(int segmentIndex, byte[] newData, int id) {
    }

    /**
     * 更新文件中某个Block的索引
     *
     * @param segmentIndex 段编号
     * @return
     */
    public void updateBlockIndex(int segmentIndex) {
    }


    public void dropTable() {
        new File(dataPath).delete();
        new File(indexPath).delete();
//        if( !(new File(dataPath).delete() && new File(indexPath).delete())){
//            throw new RuntimeException("删除表失败。");
//        }
    }

    public String getTableName() {
        return tableName;
    }

    public int getIndexHead() {
        return indexHead;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public int getDataHead() {
        return dataHead;
    }

    public void setInitialized(boolean initialized) {
        this.initialized = initialized;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public void setIndexHead(int indexHead) {
        this.indexHead = indexHead;
    }

    public void setDataHead(int dataHead) {
        this.dataHead = dataHead;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public FileChannel getIndexChannel() {
        return indexChannel;
    }

    public void setIndexChannel(FileChannel indexChannel) {
        this.indexChannel = indexChannel;
    }

    public FileChannel getDataChannel() {
        return dataChannel;
    }

    public void setDataChannel(FileChannel dataChannel) {
        this.dataChannel = dataChannel;
    }

    public static String getPublicFilePath() {
        return publicFilePath;
    }

    public static void setPublicFilePath(String publicFilePath) {
        Table.publicFilePath = publicFilePath;
    }

    public String getIndexPath() {
        return indexPath;
    }

    public void setIndexPath(String indexPath) {
        this.indexPath = indexPath;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public RandomAccessFile getIndexFile() {
        return indexFile;
    }

    public void setIndexFile(RandomAccessFile indexFile) {
        this.indexFile = indexFile;
    }

    public static String getIndexFilePrefix() {
        return indexFilePrefix;
    }

    public static void setIndexFilePrefix(String indexFilePrefix) {
        Table.indexFilePrefix = indexFilePrefix;
    }

    public RandomAccessFile getDataFile() {
        return dataFile;
    }

    public void setDataFile(RandomAccessFile dataFile) {
        this.dataFile = dataFile;
    }

    public static String getDataFilePrefix() {
        return dataFilePrefix;
    }

    public static void setDataFilePrefix(String dataFilePrefix) {
        Table.dataFilePrefix = dataFilePrefix;
    }

    public long getTableSize() {
        return tableSize;
    }

    public void setTableSize(long tableSize) {
        this.tableSize = tableSize;
    }

    public int getDataSegmentSize() {
        return dataSegmentSize;
    }

    public void setDataSegmentSize(int dataSegmentSize) {
        this.dataSegmentSize = dataSegmentSize;
    }

    public int getDataSegmentCount() {
        return dataSegmentCount;
    }

    public void setDataSegmentCount(int dataSegmentCount) {
        this.dataSegmentCount = dataSegmentCount;
    }

    public List<Integer> getAvailableDataSegmentSlot() {
        return availableDataSegmentSlot;
    }

    public void setAvailableDataSegmentSlot(List<Integer> availableDataSegmentSlot) {
        this.availableDataSegmentSlot = availableDataSegmentSlot;
    }

    public int getCurrentIndexTail() {
        return currentIndexTail;
    }

    public void setCurrentIndexTail(int currentIndexTail) {
        this.currentIndexTail = currentIndexTail;
    }

    public int getCurrentDataTail() {
        return currentDataTail;
    }

    public void setCurrentDataTail(int currentDataTail) {
        this.currentDataTail = currentDataTail;
    }

    public int getIndexSegmentSize() {
        return indexSegmentSize;
    }

    public void setIndexSegmentSize(int indexSegmentSize) {
        this.indexSegmentSize = indexSegmentSize;
    }

    public int getIndexSegmentCount() {
        return indexSegmentCount;
    }

    public void setIndexSegmentCount(int indexSegmentCount) {
        this.indexSegmentCount = indexSegmentCount;
    }

    public List<Integer> getAvailableIndexSegmentSlot() {
        return availableIndexSegmentSlot;
    }

    public void setAvailableIndexSegmentSlot(List<Integer> availableIndexSegmentSlot) {
        this.availableIndexSegmentSlot = availableIndexSegmentSlot;
    }

    public Class<Index> getIndexClass() {
        return indexClass;
    }

    public void setIndexClass(Class<Index> indexClass) {
        this.indexClass = indexClass;
    }

//    public Class<Data> getDataClass() {
//        return dataClass;
//    }

//    public void setDataClass(Class<Data> dataClass) {
//        this.dataClass = dataClass;
//    }
}
