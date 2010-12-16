package br.edu.ifpi.jazida.node.protocol;

import java.io.IOException;

import javax.xml.soap.Text;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.log4j.Logger;

import br.edu.ifpi.jazida.writable.BufferedImageWritable;
import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.opala.indexing.ImageIndexerImpl;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class ImageIndexerProtocol implements IImageIndexerProtocol {

	private static final Logger LOG = Logger.getLogger(ImageIndexerProtocol.class);

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return 0;
	}

	@Override
	public IntWritable addImage(MetaDocumentWritable metaDocument,
								BufferedImageWritable image) {
		LOG.info("Indexando imagem no servidor de RPC");
		ReturnMessage message = ImageIndexerImpl.getImageIndexerImpl().addImage(metaDocument.getMetaDoc(), image.getBufferedImage());
		return new IntWritable(message.getCode());
	}

	@Override
	public IntWritable delImage(Text id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IntWritable updateImage(Text id, MapWritable metaDocument) {
		// TODO Auto-generated method stub
		return null;
	}

}
