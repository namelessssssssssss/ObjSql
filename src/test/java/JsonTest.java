import com.alibaba.fastjson.JSON;
import com.objsql.common.message.HeadMessage;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;


@Slf4j
public class JsonTest {
    @Test
    void testParseClass(){
        Class message =
                JSON.parseObject(JSON.toJSONString(HeadMessage.class),Class.class);
        System.out.println(message.getDeclaredFields());
        System.out.println(JSON.toJSONString(HeadMessage.class));
    }
}
