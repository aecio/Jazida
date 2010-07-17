package br.edu.ifpi.jazida.node;

import java.io.Serializable;

/**
 * Representa o status de um {@link DataNode}. Guarda informações sobre o estado
 * do mesmo como: hostname e o endereço IP.
 * 
 * @author Aécio Solano Rodrigues Santos
 * 
 */
public class NodeStatus implements Serializable {

	private static final long serialVersionUID = 0;

	private String hostname;
	private String address; // Endereço IP do host
	private int port;

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}
	
	public void setPort(int port) {
		this.port = port;
	}

	public int getPort() {
		return port;
	}

	@Override
	public String toString() {
		return hostname + "/" + address;
	}
}
