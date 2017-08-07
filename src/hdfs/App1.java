package hdfs;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class App1 {
	/**
	 * 抛异常：unknown host: centos 原因：是因为本机没有解析主机名centos
	 */
	static final String PATH = "hdfs://centos:9000/hello";

	public static void main(String[] args) throws Exception {
		// hadoop fs -ls / 相当于 hadoop fs -ls hdfs://centos:9000/
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		final URL url = new URL(PATH);
		final InputStream in = url.openStream();
		/**
		 * @param in
		 *            表示输入流
		 * @param out
		 *            表示输出流
		 * @param buffSize
		 *            表示缓冲大小
		 * @param close
		 *            表示传输完毕之后是否关闭流
		 */
		IOUtils.copyBytes(in, System.out, 1024, true);
	}
}
