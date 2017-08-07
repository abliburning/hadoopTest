package hdfs;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class App1 {
	/**
	 * ���쳣��unknown host: centos ԭ������Ϊ����û�н���������centos
	 */
	static final String PATH = "hdfs://centos:9000/hello";

	public static void main(String[] args) throws Exception {
		// hadoop fs -ls / �൱�� hadoop fs -ls hdfs://centos:9000/
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		final URL url = new URL(PATH);
		final InputStream in = url.openStream();
		/**
		 * @param in
		 *            ��ʾ������
		 * @param out
		 *            ��ʾ�����
		 * @param buffSize
		 *            ��ʾ�����С
		 * @param close
		 *            ��ʾ�������֮���Ƿ�ر���
		 */
		IOUtils.copyBytes(in, System.out, 1024, true);
	}
}
