package br.edu.ifpi.jazida.node;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;

import br.edu.ifpi.jazida.writable.CollectorWritable;
import br.edu.ifpi.jazida.writable.DocumentWritable;
import br.edu.ifpi.jazida.writable.ExplanationWritable;
import br.edu.ifpi.jazida.writable.FieldSelectorWritable;
import br.edu.ifpi.jazida.writable.FilterWritable;
import br.edu.ifpi.jazida.writable.QueryWritable;
import br.edu.ifpi.jazida.writable.SortWritable;
import br.edu.ifpi.jazida.writable.TermWritable;
import br.edu.ifpi.jazida.writable.TopDocsWritable;
import br.edu.ifpi.jazida.writable.TopFieldDocsWritable;
import br.edu.ifpi.jazida.writable.WeightWritable;

public class SearchableProtocol implements ISearchableProtocol {
	
	private static final Logger LOG = Logger.getLogger(SearchableProtocol.class);
	public IndexSearcher searcher;
	
	public SearchableProtocol(IndexSearcher searcher) {
		super();
		this.searcher = searcher;
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return 0;
	}

	@Override
	public void close(){
		try {
			searcher.close();
		}catch (IOException e) {
			LOG.error(e);
		}
	}
	
	@Override
	public DocumentWritable doc(IntWritable arg0) {
		try {
			return new DocumentWritable(searcher.doc(arg0.get()));
		} catch (CorruptIndexException e) {
			LOG.error("Falha no Servidor de RPC");
			e.printStackTrace();
		} catch (IOException e) {
			LOG.error("Falha no Servidor de RPC");
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public DocumentWritable doc(IntWritable arg0, FieldSelectorWritable arg1) {
		try {
			return new DocumentWritable(searcher.doc(arg0.get(), arg1.getFieldSelector()));
		} catch (Exception e) {
			LOG.error("Falha no Servidor de RPC");
			e.printStackTrace();
		};
		return null;
	}

	@Override
	public IntWritable docFreq(TermWritable arg0) {
		try {
			return new IntWritable(searcher.docFreq(arg0.getTerm()));
		} catch (IOException e) {
			LOG.error("Falha no Servidor de RPC");
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public IntWritable[] docFreqs(TermWritable[] termsWritable) {
		try {
			Term[] terms = new Term[termsWritable.length]; 
			for (int i = 0; i < termsWritable.length; i++) {
				terms[i] = termsWritable[i].getTerm();
			}
			
			int[] docFreqs = searcher.docFreqs(terms);
			
			IntWritable[] freqs = new IntWritable[docFreqs.length];
			for (int i = 0; i < docFreqs.length; i++) {
				freqs[i] = new IntWritable(docFreqs[i]);
			}
			
			return freqs;
		} catch (IOException e) {
			LOG.error("Falha no Servidor de RPC");
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public ExplanationWritable explain(WeightWritable arg0, IntWritable arg1) {
		try {
			return new ExplanationWritable(searcher.explain(arg0.getWeight(), arg1.get()));
		} catch (IOException e) {
			LOG.error("Falha no Servidor de RPC");
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public IntWritable maxDoc() {
		try {
			return new IntWritable(searcher.maxDoc());
		} catch (IOException e) {
			LOG.error("Falha no Servidor de RPC");
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public QueryWritable rewrite(QueryWritable arg0) {
		try {
			return new QueryWritable(searcher.rewrite(arg0.getQuery()));
		} catch (IOException e) {
			LOG.error(e);
			return null;
		}
	}

	@Override
	public void search(WeightWritable arg0, FilterWritable arg1,
			CollectorWritable arg2) {
		// TODO Auto-generated method stub
		throw new java.lang.UnsupportedOperationException("Não implementado ainda....");
	}

	@Override
	public TopDocsWritable search(WeightWritable arg0, FilterWritable arg1,
			IntWritable arg2) {
		try {
			return new TopDocsWritable(searcher.search(arg0.getWeight(), arg1.getFilter(), arg2.get()));
		} catch (IOException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public TopFieldDocsWritable search(WeightWritable arg0,
			FilterWritable arg1, IntWritable arg2, SortWritable arg3) {
		try {
			return new TopFieldDocsWritable(searcher.search(arg0.getWeight(), arg1.getFilter(), arg2.get(), arg3.getSort()));
		} catch (IOException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

}
