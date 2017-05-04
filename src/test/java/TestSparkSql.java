import com.github.bigDataTools.hive.FieldInfo;
import com.github.bigDataTools.hive.FieldType;
import com.github.bigDataTools.hive.HiveTableManager;
import com.github.bigDataTools.hive.TableInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by winstone on 2016/11/18.
 */
public class TestSparkSql {

    public static void main(String[] args)throws Exception{
        TableInfo table = new TableInfo();
        table.setDataBase("myHive");
        table.setFieldSplit("\\|");
        table.setTableName("frontend");
        //table.setDataLocation("\'/test1/bb.txt\'");
        List<FieldInfo> field = new ArrayList<FieldInfo>();
        FieldInfo f1 = new FieldInfo();
        f1.setFieldIndex(1);
        f1.setFieldName("businesstype");
        f1.setFieldType(FieldType.STRING);

        FieldInfo f2 = new FieldInfo();
        f2.setFieldIndex(2);
        f2.setFieldName("id");
        f2.setFieldType(FieldType.STRING);

        FieldInfo f3 = new FieldInfo();
        f3.setFieldIndex(3);
        f3.setFieldName("c");
        f3.setFieldType(FieldType.STRING);

        FieldInfo f4 = new FieldInfo();
        f4.setFieldIndex(4);
        f4.setFieldName("d");
        f4.setFieldType(FieldType.STRING);

        FieldInfo f5 = new FieldInfo();
        f5.setFieldIndex(5);
        f5.setFieldName("e");
        f5.setFieldType(FieldType.STRING);

        FieldInfo f6 = new FieldInfo();
        f6.setFieldIndex(6);
        f6.setFieldName("f");
        f6.setFieldType(FieldType.STRING);

        FieldInfo f7 = new FieldInfo();
        f7.setFieldIndex(7);
        f7.setFieldName("g");
        f7.setFieldType(FieldType.STRING);

        field.add(f1);
        field.add(f2);
        field.add(f3);
        field.add(f4);
        field.add(f5);
        field.add(f6);
        field.add(f7);
        table.setFieldInfos(field);

        HiveTableManager service = HiveTableManager.getInstance();
        service.createExternalTable(table);
        // service.deleteTable(table);
    }
}
