package br.edu.ifpi.jazida.extras;

import java.io.File;
import java.io.IOException;

import br.edu.ifpi.jazida.client.TextIndexerClient;

public class IndexerHolder {
		private static IndexerHolder instance = new IndexerHolder(); 
		private static TextIndexerClient indexer = null;
		private static WikipediaFileReader wikiFile = null;
		
		private IndexerHolder() {
		}

		public static IndexerHolder getInstance() {
			return instance;
		}
		
		public synchronized TextIndexerClient getIndexer() {
			if (indexer == null) {
				try {
					indexer = new TextIndexerClient();
				}catch (Exception e) {
					e.printStackTrace();
				}
			}
			return indexer;
		}

		public synchronized WikipediaFileReader getFileReader() {
			if (wikiFile == null) {
				try {
					wikiFile = new WikipediaFileReader(
							new File("/home/aecio/workspace/lapesi/jazida/sample-data/wikipedia.lines.txt"),
							10000);
				} catch (IOException e) {
					e.printStackTrace();
					wikiFile = null;
				}
			}
			return wikiFile;
		}
}
