# BigDataTools
tools for bigData

提供一些操作大数据的相关类

1.EsSearchManager 封装了 elasticsearch 基本的相关操作，构建索引根据索引查询等操作：
  例如:
  
  public Boolean buildIndex(String indexName) 根据索引名称创建索引
  
  public void buildDocument(String indexName, String type, String docId, String json) 根据索引名称和索引类型创建索引
  
  public PageEntity<JSONObject> queryFulltext(List<String> keywords,List<String> indexs, List<String> types, List<String> fieldNames,
			List<String> allColumns, int pagenum, int pagesize) 根据关键字分页批量查询数据
      
 2.HdfsManager 封装了 hadoop hdfs 基本的文件操作，创建文件，上传，下载文件等基本操作:
 
  例如：

   public static void createAndAppendFile(String hdfsPath,String content) 根据文件路径创建或者是新增
   
   public static boolean uploadFile(File file, String hdfsPath, boolean overwrite) 上传文件
   
   public static boolean downloadFile(String hdfsPath, String localPath, boolean overwrite) 下载文件
   
   public static boolean delete(Path path) 删除文件
   
   public static void moveFile(String srcPath, String destPath) 移动文件
    
 3.ZookeeperManager 封装了 zookeeper 基本的操作，在zk上创建节点，删除节点，查询节点等操作
   
   例如：
   
   public void createPersinstentNode(String path,byte[] data) 创建永久节点
   
   public void createEphemeralNode(String path,byte[] data)  创建临时节点
   
   public byte[] getData(String path) 获取节点数据
   
  4.HbaseManager 封装了操作 hbase 的基本操作，根据名称和列族创建表,创建表并预分区等操作，创建表自动构建协处理器
     
     例如：
     
     public void createTable(String tableName, String...familyColumn) 创建普通表
     
     public void createTableWithSplits(String tableName, String...familyColumn) 创建表，并且创建好预分区
      
      
   5.HiveTableManager 使用spark整合hive 操作hive的工具类，前提是使用开启spark的thrif接口，封装了基本的构操作表的方法
    
     例如：
     
     public boolean createExternalTable(TableInfo tableInfo) 根据tabelInfo 创建hive的外部表
     
     public boolean createInnerTable(TableInfo tableInfo) 创建内部表
     
     
     注意:
        
         所有的基本配置都是放置到了common.properties中，使用hiveTableManager必须要开启hive和spark的thrift接口才能使用,hadoop的core-site.xml
         hdfs-site.xml 也放置到resource目录中，在zk包下也封装了zookeeper实现的分布式锁
         
         
        
        
      
      
     
    
   
    
     
   
   
  
  
   
   
    
    

