package br.edu.ifpi.jazida.node.protocol;

import static br.edu.ifpi.jazida.writable.WritableUtils.convertMapWritableToMap;

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
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class TextIndexerProtocol implements ITextIndexerProtocol {

	private TextIndexer textIndexer;
	
	public TextIndexerProtocol(TextIndexer indexer) {
		this.textIndexer = indexer;
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return 0;
	}

	@Override
	public IntWritable addText(MetaDocumentWritable metaDocWrapper, Text content) {
		MetaDocument metadoc = metaDocWrapper.getMetaDoc();
		ReturnMessage result = textIndexer.addText(metadoc, content.toString());
		return new IntWritable(result.getCode());
	}

	@Override
	public void backupNow() throws FileNotFoundException, IOException {
		textIndexer.backupNow();
	}

	@Override
	public IntWritable delText(Text identifier) {
		ReturnMessage result = textIndexer.delText(identifier.toString());
		return new IntWritable(result.getCode());
	}

	@Override
	public void optimize() throws CorruptIndexException, LockObtainFailedException, IOException {
		textIndexer.optimize();
	}

	@Override
	public void restoreBackup() {
		textIndexer.restoreBackup();
	}

	@Override
	public IntWritable updateText(Text id, MapWritable updatesWritable) {
		Map<String, String> updates = convertMapWritableToMap(updatesWritable);
		ReturnMessage result = textIndexer.updateText(id.toString(), updates);
		return new IntWritable(result.getCode());
	}
}
