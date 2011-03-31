package br.edu.ifpi.jazida.extras;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import br.edu.ifpi.jazida.client.TextIndexerClient;
import br.edu.ifpi.jazida.extras.WikipediaFileReader.WikiDocument;

public class WikipediaFileIndexer {
	private static int THREADS = 300;
	private static int MAX_LINES = 5000;
//	private static int MAX_LINES = Integer.MAX_VALUE;
	private int indexedDocs = 0;

	public static void main(String[] args) throws Exception {
		if(args.length>0) {
			MAX_LINES = Integer.parseInt(args[1]);
			new WikipediaFileIndexer().start(args[0]);
		}else {
			new WikipediaFileIndexer().start("./sample-data/wikipedia.lines.txt");
		}
		System.exit(1);
	}
	
	public void start(String fileName) throws Exception {
		TextIndexerClient indexer = new TextIndexerClient();
		WikipediaFileReader wikiFile = new WikipediaFileReader(new File(fileName), MAX_LINES);
		
		ExecutorService executor = Executors.newCachedThreadPool();
		
		long inicio = System.currentTimeMillis();
		List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
		
		for(int i=0;i<THREADS;i++) {
			Future<Integer> future = executor.submit(new IndexerTask(indexer, wikiFile));
			futures.add(future);
		}
		int totalDocumentos = 0;
		for(Future<Integer> result: futures) {
			Integer numDocs = result.get();
			totalDocumentos += numDocs;
		}
		double tempoDeExecucao = (System.currentTimeMillis() - inicio);
		System.out.println(totalDocumentos+" documentos indexados em "+tempoDeExecucao+" ms");
		System.out.println("Througput: "+(totalDocumentos/tempoDeExecucao)+" docs/seg");
		
	}

	class IndexerTask implements Callable<Integer>{
		private final TextIndexerClient indexer;
		private final WikipediaFileReader wikiFile;
		private int docs = 0;
		
		public IndexerTask(TextIndexerClient indexer, WikipediaFileReader wikiFile) {
			this.indexer = indexer;
			this.wikiFile = wikiFile;
		}

		@Override
		public Integer call() throws Exception {
			WikiDocument doc;
			while((doc = wikiFile.nextDocument()) != null) {
				indexer.addText(doc.getMetadoc(), doc.getContent());
				docs++;
				indexedDocs++;
			}
			return docs;
		}
		
	}
}
