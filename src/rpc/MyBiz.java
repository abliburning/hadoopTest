package rpc;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

public class MyBiz implements VersionedProtocol, MyBizable{

	/* (non-Javadoc)
	 * @see rpc.MyBizable#sayHello(java.lang.String)
	 */
	@Override
	public String sayHello(String name) {
		System.out.println("我被调用了！");
		return "Hello! " + name;
	}

	/* (non-Javadoc)
	 * @see rpc.MyBizable#getProtocolVersion(java.lang.String, long)
	 */
	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
			throws IOException {
		// TODO Auto-generated method stub
		return VERSION;
	}
}
