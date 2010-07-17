package br.edu.ifpi.jazida.util;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Serializer {
	
	public static Object toObject(byte[] bytes)
	throws IOException, ClassNotFoundException {
		return Serializer.toObject(bytes, 0);
	}
	
	public static Object toObject(byte[] bytes, int start)
	throws IOException, ClassNotFoundException {

		if (bytes == null || bytes.length == 0 || start >= bytes.length) {
			return null;
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		bais.skip(start);
		ObjectInputStream ois = new ObjectInputStream(bais);

		Object bObject = ois.readObject();

		bais.close();
		ois.close();

		return bObject;
	}

	public static byte[] fromObject(Serializable toBytes)
	throws IOException {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);

		oos.writeObject(toBytes);
		oos.flush();

		byte[] objBytes = baos.toByteArray();

		baos.close();
		oos.close();

		return objBytes;
	}
}
