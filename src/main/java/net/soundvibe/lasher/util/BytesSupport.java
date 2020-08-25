package net.soundvibe.lasher.util;

import java.lang.invoke.*;
import java.nio.*;

public final class BytesSupport {

	private BytesSupport() {}

	public static final ByteOrder BYTE_ORDER = ByteOrder.nativeOrder();

	private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, BYTE_ORDER);
	private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, BYTE_ORDER);

	public static byte[] toBytes(long i) {
		var result = new byte[8];
		LONG_HANDLE.set(result, 0, i);
		return result;
	}

	public static long longFromBytes(byte[] b) {
		if (b == null)
			return -1L;

		return (long)LONG_HANDLE.get(b, 0);
	}

	public static int intFromBytes(byte[] b, int fromBytes) {
		if (b == null)
			return -1;

		return (int)INT_HANDLE.get(b, fromBytes);
	}

	public static byte[] longToBytes(long i) {
		var buf = ByteBuffer.allocate(Long.BYTES);
		buf.order(BYTE_ORDER);
		buf.putLong(i);
		return buf.array();
	}

	public static long bytesToLong(byte[] b) {
		if (b == null)
			return -1;
		var buf = ByteBuffer.wrap(b);
		buf.order(BYTE_ORDER);
		return buf.getLong();
	}

	public static byte[] intToBytes(int i) {
		var buf = ByteBuffer.allocate(Integer.BYTES);
		buf.order(BYTE_ORDER);
		buf.putInt(i);
		return buf.array();
	}

	public static int bytesToInt(byte[] b) {
		if (b == null)
			return -1;
		var buf = ByteBuffer.wrap(b);
		buf.order(BYTE_ORDER);
		return buf.getInt();
	}
}