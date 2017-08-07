package rpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class MyClient {
	public static void main(String[] args) throws Exception {
		/**
		 * Construct a client-side proxy object that implements the named
		 * protocol, talking to a server at the named address.
		 */
		MyBizable proxy = (MyBizable) RPC.waitForProxy(MyBizable.class,
				MyBizable.VERSION, new InetSocketAddress(MyServer.ADDRESS,
						MyServer.PORT), new Configuration());
		final String result = proxy.sayHello("MMP");
		System.out.println("客户端结果：" + result);
		RPC.stopProxy(proxy);
	}
}
