package net.soundvibe.lasher.util;

import java.lang.invoke.*;
import java.nio.*;

public final class BytesSupport {

	private BytesSupport() {}

	public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

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

	public static byte[] longToBytes(long a) {
		return new byte[] {
				(byte) a,
				(byte) (a >> 8),
				(byte) (a >> 16),
				(byte) (a >> 24),
				(byte) (a >> 32),
				(byte) (a >> 40),
				(byte) (a >> 48),
				(byte) (a >> 56)};
	}

	public static long bytesToLong(byte[] b) {
		return  ((long) b[7] << 56)
				| ((long) b[6] & 0xff) << 48
				| ((long) b[5] & 0xff) << 40
				| ((long) b[4] & 0xff) << 32
				| ((long) b[3] & 0xff) << 24
				| ((long) b[2] & 0xff) << 16
				| ((long) b[1] & 0xff) << 8
				| ((long) b[0] & 0xff);

	}

	public static byte[] intToBytes(int a) {
		return new byte[] {
				(byte) (a & 0xFF),
				(byte) ((a >> 8) & 0xFF),
				(byte) ((a >> 16) & 0xFF),
				(byte) ((a >> 24) & 0xFF),
		};
	}

	public static int bytesToInt(byte[] b) {
		return   b[0] & 0xFF |
				(b[1] & 0xFF) << 8 |
				(b[2] & 0xFF) << 16 |
				(b[3] & 0xFF) << 24;
	}
}