package br.edu.ifpi.jazida;

import java.net.InetAddress;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;

import br.edu.ifpi.jazida.client.TextSearcherClient;
import br.edu.ifpi.jazida.node.DataNode;
import br.edu.ifpi.opala.searching.ResultItem;
import br.edu.ifpi.opala.searching.SearchResult;
import br.edu.ifpi.opala.utils.QueryMapBuilder;
/**
 * Realiza interações com um cluster Jazida através da linha de comando.
 * 
 * @author aecio
 *
 */
public class Jazida {
	
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure(null);
		if(args[0] == null) {
			System.out.println("Informe um dos metodos a ser invocado: startNode, search");
		}
		if(args[0].equals("startNode")) {
			startDataNode(args);
		}
		else if(args[0].equals("search")) {
			if(args.length < 2) {
				System.out.println("Uso: search <query>");
				System.exit(1);
			}
			search(args[1]);
		}
	}

	private static void startDataNode(String[] args) {
		final DataNode datanode = new DataNode();
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					datanode.stop();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}));
		try {
			if(args.length == 1) {
				System.out.println("\n\nsimples...\n\n");
				datanode.start();
			}else {
				try {
					System.out.println("\n\navancado...\n\n");
					datanode.start(	args[1],
								InetAddress.getLocalHost().getHostAddress(),
								Integer.parseInt(args[2]),
								Integer.parseInt(args[3]),
								Integer.parseInt(args[4]),
								Integer.parseInt(args[5]),
								true);
				}catch (NumberFormatException e) {
					System.out.println("Uso: startNode <NodeName> <TextIndexerPort> <TextSearcherPort> <ImageIndexerPort> <ImageSearcherPort>");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void search(String string) throws Exception {
		TextSearcherClient searcher = new TextSearcherClient();
		Map<String, String> fields = new QueryMapBuilder()
												.id(string)
												.author(string)
												.title(string)
												.keywords(string)
												.content(string)
												.build();
		SearchResult result = searcher.search(fields, null, 1, 10, null);
		if(result.getItems().size() > 0) {
			System.out.println("Documentos encontrados:");
			int i=0;
			for (ResultItem hit : result.getItems()) {
				i++;
				System.out.println(i+" - "+hit.getId());
			}
		}else {
			System.out.println("Nenhum documento encontrado.");
		}
		searcher.close();
		System.exit(1);
	}
}
