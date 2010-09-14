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
	private int textSearchServerPort;
	private int textIndexerServerPort;
	
	public NodeStatus(String hostname, String address, int textIndexerServerPort, int textSearchServerPort) {
		this.hostname = hostname;
		this.address = address;
		this.textIndexerServerPort = textIndexerServerPort;
		this.textSearchServerPort = textSearchServerPort;
	}

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
	
	public int getTextSearchServerPort() {
		return textSearchServerPort;
	}

	public void setTextSearchServerPort(int textSearchServerPort) {
		this.textSearchServerPort = textSearchServerPort;
	}

	public int getTextIndexerServerPort() {
		return textIndexerServerPort;
	}

	public void setTextIndexerServerPort(int textIndexerServerPort) {
		this.textIndexerServerPort = textIndexerServerPort;
	}

	@Override
	public String toString() {
		return hostname + "/" + address;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((address == null) ? 0 : address.hashCode());
		result = prime * result
				+ ((hostname == null) ? 0 : hostname.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NodeStatus other = (NodeStatus) obj;
		if (address == null) {
			if (other.address != null)
				return false;
		} else if (!address.equals(other.address))
			return false;
		if (hostname == null) {
			if (other.hostname != null)
				return false;
		} else if (!hostname.equals(other.hostname))
			return false;
		return true;
	}
	
	
}
