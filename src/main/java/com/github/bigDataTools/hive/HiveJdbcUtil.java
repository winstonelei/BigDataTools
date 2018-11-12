package com.github.bigDataTools.hive;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by winstone on 2016/11/18.
 */
public class HiveJdbcUtil {

    private static final Log logger = LogFactory.getLog(HiveJdbcUtil.class);

    private  static HiveJdbcUtil hiveJdbcUtil;

    private  final static String  hiveThriftUrl = "jdbc:hive2://ip:port/";

    private  final static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private  static String userName = "";

    private  static String passWord = "";

    private String url = "";

    private List<String> urls;


    private HiveJdbcUtil() throws  Exception{
        urls = Lists.newArrayList();
        Class.forName(driverName);
    }


    public static HiveJdbcUtil getInstance()throws Exception{
        if(hiveJdbcUtil == null){
            synchronized (HiveJdbcUtil.class){
                hiveJdbcUtil = new HiveJdbcUtil();
            }
        }
        return  hiveJdbcUtil;
    }

    public Connection getConnection()throws Exception{
        Connection conn = null;
        for(String url : urls){
            try {
                conn = DriverManager.getConnection(url, userName, passWord);
            }catch (Exception e){
                logger.error(e.getMessage());
            }
        }
        return conn;
    }

    public  void closeConnection(Connection conn){
        if(conn == null){
            return ;
        }
        try{
            if(!conn.isClosed()){
                conn.close();
            }
        }catch (Exception e){
            logger.error(e.getMessage());
        }
    }

    public List<Map<String, Object>> querySqlForKeyValue(String sql)
            throws Exception {
        Connection con = getConnection();
        Statement stmt = null;
        ResultSet rs = null;
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        try {
            stmt = con.createStatement();
            rs = stmt.executeQuery(sql);
            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();
            while (rs.next()) {
                Map<String, Object> map = Maps.newHashMap();
                for (int i = 1; i <= columnCount; i++) {
                    map.put(md.getColumnName(i), rs.getObject(i));
                }
                list.add(map);
            }
        } catch (SQLException ex) {
            logger.error("查询sql失败:" + sql, ex);
            throw new SQLException("查询sql失败:" + sql);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (con != null) {
                closeConnection(con);
            }
        }
        return list;
    }

    public boolean excuteSql(String sql) throws Exception {
        Connection con = getConnection();
        boolean flag = false;
        Statement stmt = null;
        try {
            stmt = con.createStatement();
            flag = stmt.execute(sql);
        } catch (SQLException e) {
            logger.error("执行sql失败:" + sql, e);
            throw new SQLException("执行sql失败:" + sql);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (con != null) {
                closeConnection(con);
            }
        }
        return flag;
    }

    public boolean excuteMultSql(String[] sqls) throws Exception {
        Connection con = getConnection();
        boolean flag = false;
        Statement stmt = null;
        String sqlTemp = null;
        try {
            stmt = con.createStatement();
            if (sqls != null) {
                for (String sql : sqls) {
                    sqlTemp = sql;
                    flag = stmt.execute(sqlTemp);
                }
            }
        } catch (SQLException e) {
            logger.error("执行sql失败:" + sqlTemp, e);
            throw new SQLException("执行sql失败:" + sqlTemp);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (con != null) {
                closeConnection(con);
            }
        }
        return flag;
    }

    public List<String[]> queryRows(String sql) throws Exception {
        Connection con = getConnection();
        Statement stmt = null;
        ResultSet rs = null;
        List<String[]> list = new ArrayList<String[]>();
        try {
            stmt = con.createStatement();
            rs = stmt.executeQuery(sql);
            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();
            while (rs.next()) {
                String[] row = new String[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = (String) rs.getObject(i + 1);
                }
                list.add(row);
            }
            logger.info("表列信息:" + list.toString());
        } catch (SQLException ex) {
            logger.error("查询sql失败:" + sql, ex);
            throw new SQLException("查询sql失败:" + sql);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (con != null) {
                closeConnection(con);
            }
        }
        return list;
    }

    public  String getPassWord() {
        return passWord;
    }

    public  void setPassWord(String passWord) {
        HiveJdbcUtil.passWord = passWord;
    }

    public static HiveJdbcUtil getHiveJdbcUtil() {
        return hiveJdbcUtil;
    }

    public static void setHiveJdbcUtil(HiveJdbcUtil hiveJdbcUtil) {
        HiveJdbcUtil.hiveJdbcUtil = hiveJdbcUtil;
    }


    public String getUrl() {
        return url;
    }

    public void setUrl(String ip,String port) {
        String sparkUrl = hiveThriftUrl.replace("ip", ip).replace("port", port);
        this.url = sparkUrl;
        this.urls.add(url);
    }

    public List<String> getUrls() {
        return urls;
    }

    public void setUrls(List<String> urls) {
        this.urls = urls;
    }

    public  String getUserName() {
        return userName;
    }

    public  void setUserName(String userName) {
        HiveJdbcUtil.userName = userName;
    }

    public static void main(String[] args)throws Exception{
        long startTime = System.currentTimeMillis();
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection con = DriverManager.getConnection(
                "jdbc:hive2://node:10000/default", "root", "123");
        Statement stmt = con.createStatement();



        String sql = "select * from  fz_external_table" ;
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getInt(1)) + "\t"
                    + res.getString(2));
        }

        sql = "select count(1) from fz_external_table ";
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println("count:" + res.getString(1));
        }

        long endTime = System.currentTimeMillis();
        System.out.println("spark sql execute time ="+(endTime-startTime));
    }

}
