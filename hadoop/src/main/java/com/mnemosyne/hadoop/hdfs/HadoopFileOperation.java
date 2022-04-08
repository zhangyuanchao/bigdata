package com.mnemosyne.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;

public class HadoopFileOperation {
	
	private final static String HDFS_HOST = "hdfs://hd2test.tsht3.mc.ops:8020/";

	public static void main(String[] args) throws Exception {

		// makeDirectory();
		// uploadFile();
		// downloadFile();
		// readContent();
		deleteFile();
	}
	
	/**
	 * 创建目录
	 * @throws Exception
	 */
	private static void makeDirectory() throws Exception{
		URI uri = URI.create(HDFS_HOST);
	    FileSystem fs = FileSystem.get(uri, new Configuration());
	    fs.mkdirs(new Path(HDFS_HOST + "word"), new FsPermission(777));
	    RemoteIterator<LocatedFileStatus> ri =  fs.listFiles(new Path(HDFS_HOST), false);
	    while(ri.hasNext()){
	        LocatedFileStatus lfs = ri.next();
	        System.out.println(lfs.getPath());
	    }
	}
	
	/**
	 * 上传文件到HDFS
	 * @throws Exception
	 */
	private static void uploadFile() throws Exception{
	    FileSystem fs = FileSystem.get(URI.create(HDFS_HOST), new Configuration());
	    //上传文件的路径和名称
	    final FSDataOutputStream dos = fs.create(new Path(HDFS_HOST + "word/LITTLE_DORRIT.txt"));
	    FileInputStream fis = new FileInputStream("C:\\study\\hdfs\\LITTLE_DORRIT.txt");
	    IOUtils.copy(fis, dos);
	}
	
	/**
	 * 从HDFS下载文件
	 * @throws Exception
	 */
	private static void downloadFile() throws Exception{
	    FileSystem fs = FileSystem.get(URI.create(HDFS_HOST), new Configuration());
	    //要下载的文件
	    final FSDataInputStream dis = fs.open(new Path(HDFS_HOST + "word/LITTLE_DORRIT.txt"));
	    FileOutputStream fos = new FileOutputStream("C:\\study\\DORRIT.txt");
	    IOUtils.copy(dis, fos);
	}
	
	/**
	 * 读取文件内容
	 * @throws Exception
	 */
	private static void readContent() throws Exception{
	    String file = HDFS_HOST + "word/LITTLE_DORRIT.txt";
	    FileSystem fs = FileSystem.get(URI.create(file), new Configuration());
	    FSDataInputStream is = fs.open(new Path(file));
	    byte[] buff = new byte[1024];
	    int len = 0;
	    while((len = is.read(buff)) != -1){
	        System.out.println(new String(buff, 0, len));
	    }
	}
	
	/**
	 * 删除文件或文件夹
	 * @throws Exception
	 */
	private static void deleteFile() throws Exception{
	    FileSystem fs = FileSystem.get(URI.create(HDFS_HOST), new Configuration());
	    //删除文件-true/false(文件夹-true)
	    fs.delete(new Path(HDFS_HOST + "word/LITTLE_DORRIT.txt"), false);
	}

}
