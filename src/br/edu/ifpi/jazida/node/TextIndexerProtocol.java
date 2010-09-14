package br.edu.ifpi.jazida.node;

import static br.edu.ifpi.jazida.util.WritableUtils.convertMapWritableToMap;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.LockObtainFailedException;

import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.opala.indexing.TextIndexer;
import br.edu.ifpi.opala.indexing.TextIndexerImpl;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class TextIndexerProtocol implements ITextIndexerProtocol {

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return 0;
	}

	@Override
	public IntWritable addText(MetaDocumentWritable metaDocWrapper, Text content) {
		MetaDocument metaDocument = metaDocWrapper.getMetaDoc();
		ReturnMessage result = TextIndexerImpl.getTextIndexerImpl()
							.addText(metaDocument, content.toString());
		return new IntWritable(result.getCode());
	}

	@Override
	public void backupNow() throws FileNotFoundException, IOException {
		TextIndexerImpl.getTextIndexerImpl().backupNow();
	}

	@Override
	public IntWritable delText(Text identifier) {
		TextIndexer indexer = TextIndexerImpl.getTextIndexerImpl();
		ReturnMessage result = indexer.delText(identifier.toString());
		return new IntWritable(result.getCode());
	}

	@Override
	public void optimize() throws CorruptIndexException,
			LockObtainFailedException, IOException {
		TextIndexerImpl.getTextIndexerImpl().optimize();
	}

	@Override
	public void restoreBackup() {
		TextIndexerImpl.getTextIndexerImpl().restoreBackup();

	}

	@Override
	public IntWritable updateText(Text id, MapWritable updatesWritable) {
		Map<String, String> updates = convertMapWritableToMap(updatesWritable);
		ReturnMessage result = TextIndexerImpl.getTextIndexerImpl().updateText(id.toString(), updates);
		return new IntWritable(result.getCode());
	}
}
