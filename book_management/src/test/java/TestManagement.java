import com.namless.entity.po.Book;
import com.namless.entity.po.Lent;
import com.namless.repository.BookRepository;
import com.namless.repository.LentRepository;
import com.namless.service.impl.BookServiceImpl;
import com.namless.service.impl.LentServiceImpl;
import com.objsql.common.util.common.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TestManagement {

    public static BookServiceImpl bookService;

    public static LentServiceImpl lentService;


    @BeforeAll
    static void startServer(){

    }

    @Test
    void testBookManagement() throws IllegalAccessException {

        //创建新表
        new BookRepository(2048,1024,6,20480);
        new LentRepository(2048,1024,10,20480);

        try {
            bookService = new BookServiceImpl();
            lentService = new LentServiceImpl();
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

         //存入书籍信息
         bookService.addNewBook(new Book().setBookName("Java核心技术-卷I").setAuthor("Cay S.Horstmann").setDocCode(1L).setRemains(1).setAll(1));
         bookService.addNewBook(new Book().setBookName("Java核心技术-卷II").setAuthor("Cay S.Horstmann").setDocCode(2L).setRemains(1).setAll(1));
         bookService.addNewBook(new Book().setBookName("代码整洁之道").setAuthor("Robert C.Martin").setDocCode(3L).setRemains(1).setAll(1));
         bookService.addNewBook(new Book().setBookName("JavaWeb高级编程").setAuthor("Nicholas S.Williams").setDocCode(4L).setRemains(1).setAll(1));
         bookService.addNewBook(new Book().setBookName("深入理解Java虚拟机").setAuthor("周志明").setDocCode(5L).setRemains(1).setAll(1));

        System.err.println("每本书再添加6本库存");
        for(int k=0;k<30;k++) {
           bookService.addNewBook(new Book().setDocCode(new Long(k % 5 +1)));
        }


        System.err.println("查询添加的书籍库存");
        for(long k = 1L; k<6L; k++) {
            Book book = bookService.getBook(k);
            System.err.println(k);
            //若校验失败，会抛出异常导致测试失败
            //每本书此时应有7本库存
            Assert.isTrue(book.getRemains()== 7 && book.getAll() == 7,"书籍数量不正确");
        }

        System.err.println("模拟4个用户共借阅20本书籍");
        for(int k = 0;k<20;k++){
            //设置4个不同的借阅者
            lentService.addNewRecord(new Lent().setDocCode( new Long(k % 6 +1)).setBorrowerName("borrower-"+(k % 4)).setId(k));
        }

        System.err.println("根据借阅者名称查询借阅记录");
        for(int k = 0;k<20;k++){
            List<Lent> lent = lentService.getRecord("borrower-"+(k % 4));
            //每个用户应有5条借阅记录
            Assert.isTrue(lent.size() == 5,"用户借阅书籍数量不正确");
            System.err.println(lent);
        }

        System.err.println("查询书籍库存：");
        for(long k = 1L; k<6L; k++) {
            System.err.println(bookService.getBook(k));
        }

        System.err.println("归还借阅书籍");
        for(int k = 0;k<20;k++){
            //设置4个不同的借阅者
            lentService.removeRecord("borrower-"+(k % 4),new Long(k % 6 +1));
        }

        System.err.println("查询书籍库存：");
        for(long k = 1L; k<6L; k++) {
            Book book = bookService.getBook(k);
            System.err.println(k);
            //归还所有书籍后，每本书应有7本库存
            Assert.isTrue(book.getRemains()== 7 && book.getAll() == 7,"书籍数量不正确");
        }

        //删除一本书籍
        bookService.remove(4L);
        Book book = bookService.getBook(4L);
        //删除书籍后，不应再查询的到
        Assert.isTrue(book == null,"删除失败");
        System.err.println("删除成功");


        //通过作者查询书籍
        System.err.println("通过作者查询书籍:");
        System.err.println(bookService.getByAuthor("Cay S.Horstmann"));
        System.err.println(bookService.getByAuthor("Robert C.Martin"));
        System.err.println(bookService.getByAuthor("Nicholas S.Williams"));
        System.err.println(bookService.getByAuthor("周志明"));

    }

    @Test
    void testCache(){
        new BookRepository(2048,1024,6,20480);
        new LentRepository(2048,1024,10,20480);

        try {
            lentService = new LentServiceImpl();
            bookService = new BookServiceImpl();
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        bookService.addNewBook(new Book().setBookName("Java核心技术-卷I").setAuthor("Cay S.Horstmann").setDocCode(1L).setRemains(1).setAll(1));
        for(int k = 0;k<100000L;k++){
            bookService.addNewBook(new Book().setDocCode((long) k).setBookName("book"+k).setRemains(1).setAll(1).setAuthor("author"+k));
        }

    }
}
