package br.edu.ifpi.jazida.wrapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import br.edu.ifpi.opala.utils.MetaDocument;

public class MetaDocumentWrapper extends MetaDocument implements Writable {

	private static final long serialVersionUID = 1L;
	private MetaDocument metaDoc;

	public MetaDocumentWrapper() {
	}

	public MetaDocumentWrapper(MetaDocument metaDocument) {
		this.setMetaDoc(metaDocument);
	}

	public void setMetaDoc(MetaDocument metaDoc) {
		this.metaDoc = metaDoc;
	}

	public MetaDocument getMetaDoc() {
		return metaDoc;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		byte[] metaDocBytes = new byte[size];
		for (int i = 0; i < size; i++) {
			metaDocBytes[i] = in.readByte();
		}
		try {
			metaDoc = (MetaDocument) Serializer.toObject(metaDocBytes, 0);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Erro ao reconstruir o objeto!");
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		byte[] metaDocBytes = Serializer.fromObject(metaDoc);
		int size = metaDocBytes.length;

		out.writeInt(size);
		out.write(metaDocBytes);
	}

}
