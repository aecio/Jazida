package br.edu.ifpi.jazida.node.protocol;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;

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

public class TextSearchableProtocol implements ITextSearchableProtocol {
	
	private static final Logger LOG = Logger.getLogger(TextSearchableProtocol.class);
	public IndexSearcher searcher;
	
	public TextSearchableProtocol(IndexSearcher searcher) {
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
			LOG.error("Falha em TextSearchableProtocol.close()");
			LOG.error(e);
		}
	}
	
	@Override
	public DocumentWritable doc(IntWritable arg0) {
		try {
			return new DocumentWritable(searcher.doc(arg0.get()));
		} catch (CorruptIndexException e) {
			LOG.error("Falha em TextSearchableProtocol.doc(IntWritable)");
			LOG.error(e);
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.doc(IntWritable)");
			LOG.error(e);
		}
		return null;
	}

	@Override
	public DocumentWritable doc(IntWritable arg0, FieldSelectorWritable arg1) {
		try {
			Document doc = searcher.doc(arg0.get(), arg1.getFieldSelector());
			return new DocumentWritable(doc);
		} catch (Exception e) {
			LOG.error("Falha em TextSearchableProtocol.doc(IntWritable, FieldSelector)");
			LOG.error(e);
		};
		return null;
	}

	@Override
	public IntWritable docFreq(TermWritable arg0) {
		try {
			return new IntWritable(searcher.docFreq(arg0.getTerm()));
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.docFreqs(TermWritable[])");
			LOG.error(e);
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
			LOG.error("Falha TextSearchableProtocol.docFreqs(TermWritable[])");
			LOG.error(e);
			return null;
		}
	}

	@Override
	public ExplanationWritable explain(WeightWritable arg0, IntWritable arg1) {
		try {
			return new ExplanationWritable(searcher.explain(arg0.getWeight(), arg1.get()));
		} catch (IOException e) {
			LOG.error("Falha TextSearchableProtocol.explain()");
			LOG.error(e);
			return null;
		}
	}

	@Override
	public IntWritable maxDoc() {
		try {
			return new IntWritable(searcher.maxDoc());
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.maxDoc()");
			LOG.error(e);
			return null;
		}
	}

	@Override
	public QueryWritable rewrite(QueryWritable arg0) {
		try {
			return new QueryWritable(searcher.rewrite(arg0.getQuery()));
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.rewrite(QueryWritable)");
			LOG.error(e);
			return null;
		}
	}

	@Override
	public void search(WeightWritable arg0, FilterWritable arg1, CollectorWritable arg2) {
		//
		// TODO: Implementar search() com Collectors
		//
		String message = "search(WeightWritable, FilterWritable, CollectorWritable) NÃO IMPLEMENTADO!";
		LOG.error(message);
		throw new java.lang.UnsupportedOperationException(message);
	}

	@Override
	public TopDocsWritable search(WeightWritable arg0, FilterWritable arg1,
			IntWritable arg2) {
		try {
			TopDocs search = searcher.search(arg0.getWeight(), arg1.getFilter(), arg2.get());
			return new TopDocsWritable(search);
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.search()");
			LOG.error(e);
			return null;
		}
	}

	@Override
	public TopFieldDocsWritable search(	WeightWritable arg0,
										FilterWritable arg1,
										IntWritable arg2,
										SortWritable arg3) {
		try {
			TopFieldDocs topdocs = searcher.search(	arg0.getWeight(),
													arg1.getFilter(),
													arg2.get(),
													arg3.getSort());
			return new TopFieldDocsWritable(topdocs);
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.search(WeightWritable,FilterWritable,IntWritable,SortWritabl)");
			LOG.error(e);
			return null;
		}
	}

}
