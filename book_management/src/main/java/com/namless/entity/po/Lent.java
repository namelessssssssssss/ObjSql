package com.namless.entity.po;

import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;

/**
 * 出借记录
 */
@Data
@Accessors(chain = true)
public class Lent {
    /**
     * 记录id
     */
    private Integer id;
    /**
     * 借出的文献号
     */
    private Long docCode;

    /**
     * 借阅者姓名
     */
    private String borrowerName;

    /**
     * 最迟归还日期
     */
    private LocalDateTime returnTime;
}
