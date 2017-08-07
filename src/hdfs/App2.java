package hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class App2 {
	static final String PATH = "hdfs://centos:9000/";
	static final String DIR = "/d1";
	static final String FILE = "/d1/hello";

	public static void main(String[] args) throws Exception {
		FileSystem fileSystem = getFileSystem();
		// 创建文件夹 hadoop fs -mkdir
		// mkdir(fileSystem);
		// 上传文件 hadoop fs -put /
		// putData(fileSystem);
		// 下载文件 hadoop fs -get src dst
		// getData(fileSystem);
		// 浏览文件夹
		final FileStatus[] listStatus = fileSystem.listStatus(new Path(DIR));
		for (FileStatus fileStatus : listStatus) {
			String isDir = fileStatus.isDir() ? "文件夹" : "文件";
			final String permission = fileStatus.getPermission().toString();
			final short replication = fileStatus.getReplication();
			final long len = fileStatus.getLen();
			final String path = fileStatus.getPath().toString();
			final String owner = fileStatus.getOwner();
			final String group = fileStatus.getGroup();
			System.out.println(isDir + "\t" + permission + "\t" + replication
					+ "\t" + len + "\t" + path + "\t" + owner + "\t" + group);
		}
		// 删除文件夹
		// delete(fileSystem);
	}

	private static void getData(FileSystem fileSystem) throws IOException {
		final FSDataInputStream in = fileSystem.open(new Path(FILE));
		IOUtils.copyBytes(in, System.out, 1024, true);
	}

	private static void putData(FileSystem fileSystem) throws IOException,
			FileNotFoundException {
		final FSDataOutputStream out = fileSystem.create(new Path(FILE));
		final FileInputStream in = new FileInputStream(
				"G:/学习视频/黑马hadoop视频教程全套/2初级班全套视频/初级班全套视频/初级班-4-HDFS的java操作方式/新建文本文档.txt");
		IOUtils.copyBytes(in, out, 1024, true);
	}

	private static void delete(FileSystem fileSystem) throws IOException {
		fileSystem.delete(new Path(DIR), true);
	}

	private static void mkdir(FileSystem fileSystem) throws IOException {
		fileSystem.mkdirs(new Path(DIR));
	}

	private static FileSystem getFileSystem() throws IOException,
			URISyntaxException {
		final FileSystem fileSystem = FileSystem.get(new URI(PATH),
				new Configuration());
		return fileSystem;
	}

}
