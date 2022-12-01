package com.namless.entity.po;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 书籍
 */
@Data
@Accessors(chain = true)
public class Book {
    /**
     * 文献号
     */
    private Long docCode;
    /**
     * 书籍名称
     */
    private String bookName;
    /**
     * 作者
     */
    private String author;
    /**
     * 现存量
     */
    private Integer remains;
    /**
     * 库存总量
     */
    private Integer all;
}
