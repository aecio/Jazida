package br.edu.ifpi.jazida.client;

import br.edu.ifpi.jazida.node.DataNode;

/**
 * Interface responsável por decidir o {@link DataNode} que indexará o próximo
 * documento. As subclasses devem implementar uma estratégia de distribuição de
 * documentos indexados entre os nós em funcionamento num determinado instante.
 * 
 * @author Aécio Santos
 * 
 * @param <T>
 */
public interface PartitionPolicy<T> {
	public T nextNode();
}