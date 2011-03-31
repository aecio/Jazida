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
	private static int threads = 300;
	private static int maxLines = 5000;
	private static File fileName = new File("./sample-data/wikipedia.lines.txt");

	private int indexedDocs = 0;
	private TextIndexerClient indexer;

	public static void main(String[] args) throws Exception {
		if(args.length>0) {

			fileName = new File(args[0]);
			maxLines = Integer.parseInt(args[1]);
			threads = Integer.parseInt(args[2]);
			
			new WikipediaFileIndexer().start(fileName, maxLines, threads);
			
		}else {
			new WikipediaFileIndexer().start(fileName, maxLines, threads);
		}
		System.exit(1);
	}
	
	public WikipediaFileIndexer() throws Exception { 
		indexer = new TextIndexerClient();
	}
	
	public long start(File fileName, int maxLines, int threads) throws Exception {
		
		WikipediaFileReader wikiFile = new WikipediaFileReader(fileName, maxLines);
		
		ExecutorService executor = Executors.newCachedThreadPool();
		
		long inicio = System.currentTimeMillis();
		List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
		
		for(int i=0;i<threads;i++) {
			Future<Integer> future = executor.submit(new IndexerTask(indexer, wikiFile));
			futures.add(future);
		}
		int totalDocumentos = 0;
		for(Future<Integer> result: futures) {
			Integer numDocs = result.get();
			totalDocumentos += numDocs;
		}
		long tempoDeExecucao = (System.currentTimeMillis() - inicio);
		
		System.out.println(totalDocumentos+" documentos indexados em "+tempoDeExecucao+" ms");
		System.out.println("Througput: "+(totalDocumentos/(tempoDeExecucao/1000.0))+" docs/seg");
		
		return tempoDeExecucao;
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
