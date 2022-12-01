package com.namless.service.impl;

import com.namless.entity.po.Book;
import com.namless.repository.BookRepository;
import com.namless.repository.LentRepository;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

@Slf4j
public class BookServiceImpl {

    private BookRepository bookRepository ;

    private LentRepository lentRepository;

    public BookServiceImpl() throws IllegalAccessException {
      this.bookRepository = new BookRepository();
      this.lentRepository = new LentRepository();
    }

    /**
     * 添加新的文献。如果文献已存在，增加其库存量
     */
    public void addNewBook(Book book) {
        if (bookRepository.isExist(book.getDocCode())) {
            Book rec = bookRepository.get(book.getDocCode());
            try {
                bookRepository.update(
                        rec.setRemains(rec.getRemains()+1).setAll(rec.getAll() + 1)
                );
            }
            catch (Exception e){
                e.printStackTrace();
            }
        } else {
            if(book.getRemains() == null){
                book.setRemains(1);
            }
            if(book.getAll() == null){
                book.setRemains(1);
            }
            bookRepository.update(book);
        }
    }

    /**
     * 根据文献号获取一本书
     *
     * @param docCode 文献号
     */
    public Book getBook(Long docCode) {
        return bookRepository.get(docCode);
    }

    /**
     * 删除某一本书
     */
    public void remove(Long docCode) {
        bookRepository.delete(docCode);
    }

    /**
     * 获取某著者下所有文献
     * @param author 作者名
     */
    public List<Book> getByAuthor(String author){
        return bookRepository.getByAuthor(author);
    }

}
