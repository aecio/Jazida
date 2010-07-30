package br.edu.ifpi.jazida.node;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.LockObtainFailedException;

import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.opala.utils.ReturnMessage;

/**
 * Interface dos métodos de indexação de texto do Servidor IPC/RPC do Jazida.
 * @author Aécio Santos
 *
 */
public interface ITextIndexerServer extends VersionedProtocol {

	public static final long versionID = 0;

	/**
	 * Assinatura para o método que realiza a adição da Texto no índice.
	 * 
	 * @param metaDocument  Os metadados associados ao texto.
	 * @param content O conteúdo do texto a ser indexado
	 * @return status Código de sucesso ou erro de acordo com a Enum {@link ReturnMessage}
	 */
	public IntWritable addText(MetaDocumentWritable metaDocument, Text content);
	
	/**
	 * Assinatura para o método que remove o texto do índice.
	 * 
	 * @param identifier - O identificador único do texto no índice.            
	 * @return status Código de sucesso ou erro de acordo com a Enum {@link ReturnMessage}
	 * 
	 */
	public IntWritable delText(Text identifier);
	
	/**
	 * Assinatura para o método que otimiza o índice.
	 *
	 * @throws CorruptIndexException
	 * @throws LockObtainFailedException
	 * @throws IOException
	 */
	public void optimize() throws CorruptIndexException, LockObtainFailedException, IOException;
	
	/**
	 * Assinatura para o método que faz o backup
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void backupNow() throws FileNotFoundException, IOException;
	
	/**
	 * Assinatura para o método que recupera o índice com o backup mais novo.
	 */
	public void restoreBackup();
	
	/**
	 * Assinatura para o método que atualiza um documento no índice
	 * @return status Código de sucesso ou erro de acordo com a Enum {@link ReturnMessage}
	 */
	public IntWritable updateText(Text id, MapWritable metaDocumentMap);
}
