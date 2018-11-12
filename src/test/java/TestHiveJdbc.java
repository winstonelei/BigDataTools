import com.github.bigDataTools.hive.HiveJdbcUtil;
import com.github.bigDataTools.hive.HiveTableManager;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 〈一句话功能简述〉<br>
 * 〈功能详细描述〉
 *
 * @author winston
 * @see [相关类/方法]（可选）
 * @since [产品/模块版本] （可选）
 */
public class TestHiveJdbc {

    public static void main(String[] args) throws Exception{
        long startTime = System.currentTimeMillis();
        HiveJdbcUtil hiveJdbcUtil = HiveJdbcUtil.getInstance();
        hiveJdbcUtil.setUrl("192.168.130.14","10000");
        hiveJdbcUtil.setUserName("root");
        hiveJdbcUtil.setPassWord("123");
        Connection connection = hiveJdbcUtil.getConnection();
        //List<Map<String, Object>> resList = hiveJdbcUtil.querySqlForKeyValue("select * from fz_external_table");
        List<Map<String, Object>> resList = hiveJdbcUtil.querySqlForKeyValue("select count(*) as totalCount from fz_external_table");
        long endTime = System.currentTimeMillis();
        System.out.println("costTime="+(endTime-startTime));
        resList.stream().forEach(item->{
            System.out.println(item.toString());
        });
    }
}
