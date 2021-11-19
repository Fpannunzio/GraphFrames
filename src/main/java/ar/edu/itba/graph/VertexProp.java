package ar.edu.itba.graph;

import java.io.Serializable;

public class VertexProp implements Serializable 
{
	private static final long serialVersionUID = 41L;
	private String URL;
	private String creator;
	

	public VertexProp(String anUrl, String aCreator)
	{
		URL= anUrl;
		creator= aCreator;
	}
	
	public String getURL()
	{
		return URL;
	}
	
	public String getCreator()
	{
		return creator;
	}
	
	@Override
	public String toString() {
		return "URL=" + URL + " creator= " + creator;
	}
}

