package com.namless.repository;

import com.namless.entity.po.Book;
import com.objsql.client.datasource.AbstractRepository;
import com.objsql.client.datasource.BaseRepository;
import com.objsql.common.message.TableCreateParam;

import java.util.List;

public class BookRepository extends BaseRepository<Long,Book> {

    public BookRepository() throws IllegalAccessException {
        super(BookRepository.class.getSimpleName(), Long.class, Book.class);
    }

    public BookRepository(int dataSegmentSize,int indexSegmentSize,int blockSize,int metadataOffset) {
        super(new TableCreateParam<>(Long.class,Book.class,BookRepository.class.getSimpleName(),dataSegmentSize,indexSegmentSize,blockSize,metadataOffset));
    }

    /**
     * 添加一本文献
     *
     * @param book 文献
     * @return 添加的是否是新文献
     */
    public boolean add(Book book) {
        Book record = this.get(book.getDocCode());
        if (record != null) {
            super.add(record.getDocCode(), record.setAll(record.getAll() + 1).setRemains(record.getRemains() + 1));
            return false;
        } else {
            super.add(book.getDocCode(), book);
            return true;
        }
    }


    /**
     * 通过文献名称查询文献
     * @param bookName 文献名称
     */
    public List<Book> getByName(String bookName) {
        try {
            return super.getByField(bookName, "bookName");
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<Book> getByAuthor(String author) {
        try {
            return super.getByField(author, "author");
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 查询一条记录是存在
     *
     * @param id id
     * @return 存在返回true，不存在返回false
     */
    public boolean isExist(Long id) {
        return super.get(id) != null;
    }

    /**
     * 更新一条记录，若记录不存在，则添加
     */
    public void update(Book book) {
       super.add(book.getDocCode(), book);
    }

    /**
     * 删除一条记录
     */
    public void delete(Long id) {
        super.delete(id);
    }


}
