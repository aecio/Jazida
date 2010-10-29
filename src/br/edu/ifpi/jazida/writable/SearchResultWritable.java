package br.edu.ifpi.jazida.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import br.edu.ifpi.jazida.nio.Serializer;
import br.edu.ifpi.opala.searching.SearchResult;

public class SearchResultWritable implements Writable {
	
	private SearchResult searchResult;

	public SearchResultWritable() {
		//Necessário para serialização
	}
	
	public SearchResultWritable(SearchResult searchResult) {
		this.searchResult = searchResult;
	}

	/**
	 * @return the searchResult
	 */
	public SearchResult getSearchResult() {
		return searchResult;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		byte[] searchResultBytes = new byte[size];
		for (int i = 0; i < size; i++) {
			searchResultBytes[i] = in.readByte();
		}
		try {
			this.searchResult = (SearchResult) Serializer.toObject(searchResultBytes);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Erro ao reconstruir o objeto!");
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		byte[] metaDocBytes = Serializer.fromObject(this.searchResult);
		int size = metaDocBytes.length;

		out.writeInt(size);
		out.write(metaDocBytes);
	}
}
