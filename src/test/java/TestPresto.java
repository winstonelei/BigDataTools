import com.github.bigDataTools.hive.HiveJdbcUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 〈一句话功能简述〉<br>
 * 〈功能详细描述〉
 *  presto查询
 * @author winston
 * @see [相关类/方法]（可选）
 * @since [产品/模块版本] （可选）
 */
public class TestPresto {

    @Test
    public void testQuery() throws  Exception{
        long startTime = System.currentTimeMillis();
        Class.forName("com.facebook.presto.jdbc.PrestoDriver");
        Connection connection = DriverManager.getConnection("jdbc:presto://192.168.130.14:8080/hive/default","root","");  ;
        Statement stmt = connection.createStatement();
        //  ResultSet rs = stmt.executeQuery("select count(*) from fz_external_table_withtime");  select sum(income) as income,sum(expenses) as expenses from trade_detail
        ResultSet rs = stmt.executeQuery("select sum(income) as income,sum(expenses) as expenses from trade_detail");
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        rs.close();
        connection.close();
        long endTime = System.currentTimeMillis();
        System.out.println("costTime="+(endTime-startTime));
    }


}
