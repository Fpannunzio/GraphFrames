package ar.edu.itba.graph;

import java.io.Serializable;

public class NewVertexProp implements Serializable {

    private static final long serialVersionUID = 41L;
	private String URL;
	private boolean parity;
	

	public NewVertexProp(String anUrl, boolean parity)
	{
		URL= anUrl;
		this.parity = parity;
	}
	
	public String getURL()
	{
		return URL;
	}
	
	public boolean getParity()
	{
		return parity;
	}
	
	@Override
	public String toString() {
		return "URL=" + URL + " parity= " + parity;
	}
    
}
