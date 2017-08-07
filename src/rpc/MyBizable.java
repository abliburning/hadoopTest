package rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyBizable extends VersionedProtocol {
	long VERSION = 666L;

	public abstract String sayHello(String name);

}