package br.edu.ifpi.jazida.client;

public interface PartitionPolicy<T> {
	public T nextNode();
}