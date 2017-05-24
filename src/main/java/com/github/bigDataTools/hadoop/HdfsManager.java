package com.github.bigDataTools.hadoop;

import org.apache.commons.io.FileExistsException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ResourceBundle;

/**
 * Created by winstone on 2017/5/4.
 * operate for hdfs
 */
public class HdfsManager {

    protected final static Logger LOG = LoggerFactory.getLogger(HdfsManager.class);
    protected static FileSystem fs;
    protected static Configuration conf;
    protected static final ResourceBundle rb ;
    protected static final String HDFS_DATACENTER = "hdfs.data.center";
    protected static final Path data_location ;

    static {
        rb = ResourceBundle.getBundle("commons");
        if (rb == null) {
            throw new RuntimeException("hadoop.properties配置文件不存在");
        }
        String dataCenter = rb.getString(HDFS_DATACENTER);
        if (StringUtils.isBlank(dataCenter)) {
            throw new RuntimeException("中心根目录在配置文件中为空或不存在");
        }
        data_location = new Path(dataCenter);
        conf = new Configuration();
        try {
         fs=FileSystem.get(conf);;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 在根目录下创建文件夹
     * @param dirName 指定文件路径
     * @return
     * @throws IOException
     */
    public static boolean mkRootDir(String dirName) throws IOException {
        boolean flag = false;
        if (StringUtils.isBlank(dirName)) {
            throw new RuntimeException();
        }
        Path path = new Path(data_location.toString(), dirName);
        if (fs.exists(path)) {
            throw new FileExistsException(dirName);
        }
        flag = fs.mkdirs(path);
        return flag;
    }

    /**
     * 下载文件-根据相对用户路径，下载指定用户目录下的文件到指定本地路径
     * @param hdfsPath
     * @param localPath
     * @param overwrite
     * @return
     * @throws IOException
     */
    public static boolean downloadFile(String hdfsPath, String localPath, boolean overwrite) throws IOException {
        boolean flag = true;
        Path oldPath = new Path(hdfsPath);
        File file = new File(localPath);
        if (!fs.exists(oldPath)) {
            throw new RuntimeException(hdfsPath);
        }
        if (file.exists() && overwrite) {
            FileUtil.fullyDelete(file);
        }
        if (file.exists() && !overwrite) {
            throw new FileExistsException(hdfsPath);
        }
        flag = FileUtil.copy(fs, oldPath, file, false, conf);
        return flag;
    }


    /**
     * 上传文件
     *
     * @param file
     * @param hdfsPath
     * @return
     * @throws IOException
     */
    public static boolean uploadFile(File file, String hdfsPath, boolean overwrite) throws IOException {
        Path oldPath = new Path(hdfsPath);
        if (!fs.exists(oldPath)) {
            mkDir(hdfsPath, true);
        }
        Path filePath = new Path(oldPath, file.getName());
        // 判断文件存在，覆盖，则删除已经存在的文件
        if (fs.exists(filePath) && overwrite) {
            delete(filePath);
        }
        // 判断文件存在，不覆盖，则直接返回FALSE
        if (fs.exists(filePath) && !overwrite) {
            throw new FileExistsException(hdfsPath);
        }
        boolean flag = true;
        flag = FileUtil.copy(file, fs, oldPath, false, conf);
        return flag;
    }


    /**
     * 根据文件路径创建或者是新增
     * @param hdfsPath
     * @param content
     */
    public static void createAndAppendFile(String hdfsPath,String content)throws Exception{
        Path path = new Path(hdfsPath);
        if(!fs.exists(path)){
            fs.createNewFile(path);
        }
        FSDataOutputStream output=fs.append(path);
        //追加
        output.write(content.getBytes());
        output.close();
        LOG.info("创建文件或者追加文件成功.....");
    }

    /**
     * 根据PATH删除文件
     *
     * @param path 路径
     * @return
     * @throws IOException
     */
    public static boolean delete(Path path) throws IOException {
        return fs.delete(path, true);
    }

    /**
     * 根据全路径创建文件夹
     * @param fullPath
     * @param rename
     * @return
     * @throws IOException
     */
    public static boolean mkDir(String fullPath, boolean rename)
            throws IOException {
        boolean flag = true;
        Path path = new Path(fullPath);
        if (fs.exists(path)) {
            flag = false;
        }
        if (!flag && rename) {
            path = folderRename(path);
            flag = true;
        } else if (!flag && !rename) {
            return flag;
        }
        if (flag) {
            flag = fs.mkdirs(path);
        }
        return flag;
    }


    /**
     * 移动文件-根据用户名下的文件来移动
     *
     * @return
     * @throws IOException
     */
    public static void moveFile(String srcPath, String destPath)
            throws IOException {
        Path srcBasePath = new Path(srcPath);
        Path dstBasePath = new Path(destPath);
        fs.copyFromLocalFile(srcBasePath, dstBasePath);
    }

    /**
     * 查询对应路径下的文件
     *
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static FileStatus[] queryPathFiles(Path queryDir)
            throws Exception {
        FileStatus[] filesInfo = null;
        if (queryDir != null) {
            filesInfo = fs.listStatus(queryDir);
        }
        return filesInfo;
    }

    /**
     * 判断文件是否存在
     *
     * @param path
     * @return
     * @throws IOException
     */
    public static boolean exists(Path path) throws IOException {
        boolean flag = fs.exists(path);
        return flag;
    }


    /**
     * 判断文件是否存在
     *
     * @return
     * @throws IOException
     */
    public static boolean exists(String pathStr) throws IOException {
        Path path = new Path(pathStr);
        return exists(path);
    }

    /**
     * 判读某个dirPath路径是否是文件夹
     * @param dirPath
     * @return
     * @throws IOException
     */
    public static boolean isDirectory(String dirPath)
            throws IOException {
        Path path = new Path(dirPath);
        try {
            return fs.isDirectory(path);
        } catch (IOException e) {
            throw e;
        }
    }


    /**
     * 根据指定文件夹获取文件流
     * @param fullpath
     * @return
     * @throws IOException
     */
    public static InputStream getDFSInputStream(String fullpath)
            throws IOException {
        Path path = new Path(fullpath);
        InputStream inputStream = null;
        if ((fs.exists(path)) && !(fs.isDirectory(path))) {
            // 指定要被压缩的文件路径
            FSDataInputStream in = fs.open(path);
            inputStream = in;
        }
        return inputStream;
    }


    /**
     * 流上传文件
     *
     * @param is
     * @param buffersize
     * @param path
     * @throws IOException
     */
    public static void streamInput(InputStream is, int buffersize, String path,
                                   boolean overwrite) throws IOException {
        FSDataOutputStream out = null;
        Path hpath = new Path(path);
        if (!fs.exists(hpath.getParent())) {
            fs.mkdirs(hpath.getParent());
        }
        try {
            out = fs.create(hpath, overwrite, buffersize, new Progressable() {
                @Override
                public void progress() {
                 //do nothing
                }
            });
            IOUtils.copyBytes(is, out, buffersize, true);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        } finally {
            IOUtils.closeStream(out);
        }
    }


    /**
     * 获取HDFS目录下的HDFS text文本文件内容
     * @param fullPath
     * @return 文本内容
     * @throws Exception
     */
    public static String getHdfsText(String fullPath) throws Exception {
        StringBuilder sb = new StringBuilder();

        FSDataInputStream ins = fs.open(new Path(fullPath));

        BufferedReader br = new BufferedReader(new InputStreamReader(ins));
        int len = 0;
        String line = null;
        while ((line = br.readLine()) != null) {

            if (len != 0) {
                sb.append("\r\n" + line);
            } else {
                sb.append(line);
            }
            len++;
        }

        ins.close();
        br.close();

        return sb.toString();
    }

    /**
     * 获取已达成har包的文件内容
     * @param filePath
     * @param fileName
     * @return 文件内容
     * @throws Exception
     */
    public static String getTxtHarFileString(String filePath, String fileName)
            throws Exception {

        HarFileSystem fs = new HarFileSystem();
        fs.initialize(new URI(filePath), conf);

        FSDataInputStream ins = fs.open(new Path(fileName), 1024);

        StringBuilder sb = new StringBuilder();

        BufferedReader br = new BufferedReader(new InputStreamReader(ins));

        int len = 0;
        String line = null;

        while ((line = br.readLine()) != null) {

            if (len != 0) {
                sb.append("\r\n" + line);
            } else {
                sb.append(line);
            }
            len++;
        }
        ins.close();
        br.close();
        return sb.toString();
    }

    /**
     * 重命名文件夹-如果重名则在后面添加数字
     * @param dirPath
     * @return
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public static Path folderRename(Path dirPath)
            throws IllegalArgumentException, IOException {
        Integer i = 1;
        Path tempPath = dirPath;
        do {
            String temp = tempPath.toString();
            if (fs.exists(tempPath) && i.equals(1)) {
                String path = temp + "(" + i + ")";
                tempPath = new Path(path);
            } else if (fs.exists(tempPath)) {
                int j = temp.lastIndexOf("(" + (i - 1) + ")");
                if (j != -1) {
                    String str = temp.substring(0, j);
                    String path = str + "(" + i + ")";
                    tempPath = new Path(path);
                }
            } else {
                break;
            }
            i++;
        } while (i < Integer.MAX_VALUE);
        return tempPath;
    }
}
