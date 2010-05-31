package br.edu.ifpi.jazida.api;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

import br.edu.ifpi.jazida.wrapper.MetaDocumentWrapper;

public interface IOpalaTextIndexer extends VersionedProtocol {

	public static final long versionID = 0;

	public IntWritable addText(MetaDocumentWrapper metaDocument, Text content);
	
}
