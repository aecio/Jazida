package br.edu.ifpi.jazida.extras;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.MetaDocumentBuilder;

class WikipediaFileReader {

	private BufferedReader reader;
	private int lineNumber = 0;
	private int maxLines;
	
	public WikipediaFileReader(File file) throws IOException {
		this(file, Integer.MAX_VALUE);
	}

	public WikipediaFileReader(File file, int maxLines) throws IOException {
		this.reader = new BufferedReader(new FileReader(file));
		this.maxLines = maxLines;
	}
	
	public WikiDocument nextDocument() throws IOException {
		String line;
		WikiDocument doc = null;
		while(doc == null) {
			int id;
			synchronized(reader) {
				line = reader.readLine();
				lineNumber++;
				id = lineNumber;
			}
			if(line == null || lineNumber > maxLines)
				return null;
			
			doc = parseWikiDocument(id, line);
		}
		return doc;
	}

	private WikiDocument parseWikiDocument(int id, String linha) {
		try {
			StringTokenizer tokenizer = new StringTokenizer(linha, "\t");
			
			String title = tokenizer.nextToken();
			String date = tokenizer.nextToken();
			String content = tokenizer.nextToken();
			
			MetaDocument metadoc = new MetaDocumentBuilder()
										.id(String.valueOf(id))
										.title(title)
										.publicationDate(date)
										.build();
			
			return new WikiDocument(metadoc, content);
			
		}catch (NoSuchElementException e) {
			System.out.println("Falhou ao ler documento "+id);
		}
		return null;
	}
	
	public class WikiDocument {
		private final MetaDocument metadoc;
		private final String content;

		public WikiDocument(MetaDocument metadoc, String conteudo) {
			this.metadoc = metadoc;
			this.content = conteudo;
		}
		public MetaDocument getMetadoc() {
			return metadoc;
		}
		public String getContent() {
			return content;
		}
	}

}
