package br.edu.ifpi.jazida.client;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searchable;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;

import br.edu.ifpi.jazida.node.protocol.ITextSearchableProtocol;
import br.edu.ifpi.jazida.writable.FieldSelectorWritable;
import br.edu.ifpi.jazida.writable.FilterWritable;
import br.edu.ifpi.jazida.writable.QueryWritable;
import br.edu.ifpi.jazida.writable.SortWritable;
import br.edu.ifpi.jazida.writable.TermWritable;
import br.edu.ifpi.jazida.writable.WeightWritable;

public class RemoteSearchableAdapter implements Searchable {
	
	ITextSearchableProtocol searchableProxy;
	
	public RemoteSearchableAdapter(ITextSearchableProtocol searchableProxy) {
		this.searchableProxy = searchableProxy;
	}

	@Override
	public void close() throws IOException {
		searchableProxy.close();
	}

	@Override
	public Document doc(int arg0) throws CorruptIndexException, IOException {
		return searchableProxy.doc(new IntWritable(arg0)).getDocument();
	}

	@Override
	public Document doc(int arg0, FieldSelector arg1)
			throws CorruptIndexException, IOException {
		return searchableProxy.doc(new IntWritable(arg0), new FieldSelectorWritable(arg1)).getDocument();
	}

	@Override
	public int docFreq(Term arg0) throws IOException {
		return searchableProxy.docFreq(new TermWritable(arg0)).get();
	}

	@Override
	public int[] docFreqs(Term[] terms) throws IOException {
		TermWritable[] termsWritable = new TermWritable[terms.length]; 
		for (int i = 0; i < terms.length; i++) {
			termsWritable[i] = new TermWritable(terms[i]);
		}
		
		IntWritable[] docFreqs = searchableProxy.docFreqs(termsWritable);
		
		int[] freqs = new int[docFreqs.length];
		for (int i = 0; i < docFreqs.length; i++) {
			freqs[i] = docFreqs[i].get();
		}
		return freqs;
	}

	@Override
	public Explanation explain(Weight arg0, int arg1) throws IOException {
		return searchableProxy.explain(new WeightWritable(arg0), new IntWritable(arg1)).getExplanation();
	}

	@Override
	public int maxDoc() throws IOException {
		return searchableProxy.maxDoc().get();
	}

	@Override
	public Query rewrite(Query arg0) throws IOException {
		return searchableProxy.rewrite(new QueryWritable(arg0)).getQuery();
	}

	@Override
	public void search(Weight arg0, Filter arg1, Collector arg2) throws IOException {
		//
		// TODO: implementar busca com Collectors
		//
		throw new UnsupportedOperationException("Operação ainda não implementada!");
	}

	@Override
	public TopDocs search(Weight arg0, Filter arg1, int arg2)
			throws IOException {
		return searchableProxy.search(new WeightWritable(arg0), new FilterWritable(arg1), new IntWritable(arg2)).getTopDocs();
	}

	@Override
	public TopFieldDocs search(Weight arg0, Filter arg1, int arg2, Sort arg3)
			throws IOException {
		return searchableProxy.search(new WeightWritable(arg0), new FilterWritable(arg1), new IntWritable(arg2), new SortWritable(arg3)).getTopFieldDocs();
	}

	
}
