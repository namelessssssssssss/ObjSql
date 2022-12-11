package com.namless.service.impl;

import com.namless.entity.po.Book;
import com.namless.entity.po.Lent;
import com.namless.repository.BookRepository;
import com.namless.repository.LentRepository;
import com.namless.service.LentService;

import java.util.List;

public class LentServiceImpl implements LentService {

    private final BookRepository bookRepository ;

    private final LentRepository lentRepository;

    public LentServiceImpl() throws IllegalAccessException {
        this.bookRepository = new BookRepository();
        this.lentRepository = new LentRepository();
    }

    /**
     * 添加一条新的借阅记录
     *
     * @param lent 借阅记录
     */
    public boolean addNewRecord(Lent lent) {
        Book book = bookRepository.get(lent.getDocCode());
        if (book != null && book.getRemains() > 0) {
            book.setRemains(book.getRemains() - 1);
            bookRepository.update(book);
            lentRepository.add(lent);
            return true;
        } else {
            return false;
        }
    }

    /**
     * 为用户归还一本书
     *
     * @param borrowerName 借阅者名称
     * @param docCode      文献号
     */
    public boolean removeRecord(String borrowerName, Long docCode) {
        List<Lent> records = lentRepository.getByBorrowerName(borrowerName);
        for (Lent record : records) {
            if (record.getDocCode().equals(docCode)) {
                Book book = bookRepository.get(record.getDocCode());
                bookRepository.update(book.setRemains(book.getRemains() + 1));
                return true;
            }
        }
        return false;
    }

    /**
     * 通过借阅者id查询借阅记录
     *
     * @param id 借阅记录id
     * @return 借阅记录
     */
    public Lent getRecord(int id) {
        return lentRepository.get(id);
    }

    /**
     * 获取某人借阅的所有书
     *
     * @param borrowerName 借阅者名称
     */
    public List<Lent> getRecord(String borrowerName) {
        return lentRepository.getByBorrowerName(borrowerName);
    }

}
