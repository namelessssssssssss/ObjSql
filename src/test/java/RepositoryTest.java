import com.objsql.ServerBoot;
import com.objsql.client.ClientBoot;
import com.objsql.client.datasource.Repository;
import com.objsql.common.message.TableCreateParam;

public class RepositoryTest {

    static {
        ServerBoot.main(null);
        ClientBoot.main(null);
    }

    public static void main(String[] args) throws IllegalAccessException {
        TableCreateParam<Long> param = new TableCreateParam<>(
                Long.class,
                String.class,
                "test04",
                4096,
                1024,
                1024,
                4096);
        Repository<Long,String> repository = new Repository<>(param);

        repository.add(123L,"index = 123L");

        System.out.println(repository.get(123L));

        //Repository<Long,String> repository = new Repository<>("test03",Long.class,String.class);
    }


}
