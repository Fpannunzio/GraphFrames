package ar.edu.itba.graph;

import java.io.Serializable;

public class NewEdgeProp implements Serializable
{
	private static final long serialVersionUID = 42L;
	private String label;
	private String year;
	
	public NewEdgeProp(String aLabel, String year)
	{
		label= aLabel;
		this.year= year;
	}
	
	public String getLabel()
	{
		return label;
	}
	
	public String getYear()
	{
		return year;
	}

	@Override
	public String toString() {
		return "Label= " + label + " year=" + year;
	}
}