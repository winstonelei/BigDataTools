
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.*;

/** 
* @author winston
* @version 2016年7月8日 上午10:38:49 
*/
public class TestGenerateFile {
    //生成文件路径
    private static String path = "D:\\tmp\\";

    //文件路径+名称
    private static String filenameTemp;

    /**
     * 创建文件
     *
     * @param fileName    文件名称
     * @param filecontent 文件内容
     * @return 是否创建成功，成功则返回true
     */
    public static boolean createFile(String fileName, String filecontent) {
        Boolean bool = false;
        filenameTemp = path + fileName + ".txt";//文件路径+名称+文件类型
        File file = new File(filenameTemp);
        try {
            //如果文件不存在，则创建新的文件
            if (!file.exists()) {
                file.createNewFile();
                bool = true;
                System.out.println("success create file,the file is " + filenameTemp);
                //创建文件成功后，写入内容到文件里
                writeFileContent(filenameTemp, filecontent);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return bool;
    }

    /**
     * 向文件中写入内容
     *
     * @param filepath 文件路径与名称
     * @param newstr   写入的内容
     * @return
     * @throws IOException
     */
    public static boolean writeFileContent(String filepath, String newstr) throws IOException {
        Boolean bool = false;
        String filein = newstr + "\r\n";//新写入的行，换行
        String temp = "";

        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        FileOutputStream fos = null;
        PrintWriter pw = null;
        try {
            File file = new File(filepath);//文件路径(包括文件名称)
            //将文件读入输入流
            fis = new FileInputStream(file);
            isr = new InputStreamReader(fis);
            br = new BufferedReader(isr);
            StringBuffer buffer = new StringBuffer();

            //文件原有内容
            for (int i = 0; (temp = br.readLine()) != null; i++) {
                buffer.append(temp);
                // 行与行之间的分隔符 相当于“\n”
                buffer = buffer.append(System.getProperty("line.separator"));
            }
            buffer.append(filein);

            fos = new FileOutputStream(file);
            pw = new PrintWriter(fos);
            pw.write(buffer.toString().toCharArray());
            pw.flush();
            bool = true;
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        } finally {
            //不要忘记关闭
            if (pw != null) {
                pw.close();
            }
            if (fos != null) {
                fos.close();
            }
            if (br != null) {
                br.close();
            }
            if (isr != null) {
                isr.close();
            }
            if (fis != null) {
                fis.close();
            }
        }
        return bool;
    }

    /**
     * 删除文件
     *
     * @param fileName 文件名称
     * @return
     */
    public static boolean delFile(String fileName) {
        Boolean bool = false;
        filenameTemp = path + fileName + ".txt";
        File file = new File(filenameTemp);
        try {
            if (file.exists()) {
                file.delete();
                bool = true;
            }
        } catch (Exception e) {
            // TODO: handle exception
        }
        return bool;
    }

    @Test
    public void testCreateFile() throws  Exception{
        for(int i=0;i<1;i++){
            Map<String,Object> events = new HashMap<>();
            events.put("schema","http");
            events.put("method","GET");
            events.put("timeType","1");
            events.put("sessionId","sessionId");
            events.put("serverPort","2041");
            events.put("trendyType","12");
            events.put("type","12");
            events.put("geolocationCountryCode","CN");
            events.put("url","/static/js/tj.js");
            events.put("requestDatetime",new Date());
            events.put("policyId","-jRYUcDXrdO0UKM8YDBYMw");
            events.put("clientIp","121.207.111.122");
            events.put("domain","122.gov.cn");
            events.put("domainName","fj");
            events.put("requestPolicyReference","{\"link\":\"https://localhost/mgmt/tm/asm/policies/-jRYUcDXrdO0UKM8YDBYMw?ver=12.1.2\"}");
            events.put("clientPort","28112");
            events.put("customerId","1331");
            long currentTime = new Date().getTime();

            events.put("serverIp","10.0.217.76");
            events.put("enforcementState","28112");
            events.put("time",currentTime);
            events.put("toTime",currentTime);
            //	events.put("fromTime",String.valueOf(currentTime));
            events.put("requestStatus","0");

            StringBuilder sb = new StringBuilder();
            Set<Map.Entry<String, Object>> set = events.entrySet();
            Iterator<Map.Entry<String,Object>> iterator = set.iterator();
            while(iterator.hasNext()){
                Map.Entry<String,Object> entry = iterator.next();
                sb.append(entry.getKey()+entry.getValue());
            }

            String filecontent = sb.toString();//i+",fz"+i+",2500,1318888,1541741464775";
            try {
                writeFileContent("d:\\tmp\\trade1.txt", filecontent);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        for(int i=0;i<50000;i++){
            String filecontent = i+",fz"+i+",25,13188";
            try {
                writeFileContent("d:\\tmp\\user.txt", filecontent);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
    
    
    
    
    
    
    
    
    
    
    
    
    