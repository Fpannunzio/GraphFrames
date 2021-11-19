package ar.edu.itba.graph;

import java.io.Serializable;
import java.util.Date;

public class EdgeProp implements Serializable
{
	private static final long serialVersionUID = 42L;
	private String label;
	private Date date;
	
	public EdgeProp(String aLabel, Date aDate )
	{
		label= aLabel;
		date= aDate;
	}
	
	public String getLabel()
	{
		return label;
	}
	
	public Date getDate()
	{
		return date;
	}

	@Override
	public String toString() {
		return "Label= " + label + " date=" + date;
	}
}
