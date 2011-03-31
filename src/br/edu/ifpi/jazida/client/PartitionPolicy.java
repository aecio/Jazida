package br.edu.ifpi.jazida.client;

/**
 * Interface responsável por escolher entre decidir os objetos do tipo <T>. As
 * subclasses devem implementar uma estratégia de seleção entre os nós
 * disponíveis.
 * 
 * @author Aécio Santos
 * 
 * @param <T>
 */
public interface PartitionPolicy<T> {
	public void addNode(T... nodes);

	public void removeNode(T... deadNodes);

	public T nextNode();
}