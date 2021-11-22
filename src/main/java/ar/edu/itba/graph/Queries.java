package ar.edu.itba.graph;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.sort_array;

public class Queries {

    public static final Column NEGATIVE_LATITUTE = new Column("a.lat").isNotNull().and(new Column("a.lat").lt(Double.valueOf("0")));
	public static final Column NEGATIVE_LONGITUDE = new Column("a.lon").isNotNull().and(new Column("a.lon").lt(Double.valueOf("0")));
    

    public static Dataset<Row> oneStopFlightsQuery(final GraphFrame myGraph) {
		return myGraph.filterVertices("labelV = 'airport'").filterEdges("labelE = 'route'")
		.find("(a)-[e]->(b); (b)-[e2]->(c)")
		.filter(NEGATIVE_LATITUTE)
		.filter(NEGATIVE_LONGITUDE)
		.filter("c.code = 'SEA'").filter("a.id != b.id and a.id != c.id and b.id != c.id")
        .select(col("a.code").alias("airportCode"), col("a.lat").alias("latitude")
        ,col("a.lon").alias("longitude"), col("b.code").alias("stepAirportCode"), col("c.code").alias("destAirportCode"));
	}

	public static Dataset<Row> directFlightsQuery(final GraphFrame myGraph) {
		return myGraph.filterVertices("labelV = 'airport'").find("(a)-[e]->(b)")
		.filter(NEGATIVE_LATITUTE)
		.filter(NEGATIVE_LONGITUDE)
		.filter("b.code = 'SEA'").filter("a.id != b.id")
        .select(col("a.code").alias("airportCode"), col("a.lat").alias("latitude")
        ,col("a.lon").alias("longitude"), col("b.code").alias("destAirportCode"));
	}

    public static Dataset<Row> countriesElevationsQuery(final GraphFrame myGraph) {
        return myGraph.filterEdges("labelE = 'contains'").find("(c)-[]->(a); (p)-[]->(a)")
				.filter(col("a.labelV").eqNullSafe("airport"))
                .filter(col("c.labelV").eqNullSafe("continent"))
				.filter(col("p.labelV").eqNullSafe("country"))
				.select(col("c.desc").alias("continent"), col("a.country").alias("country"),
						col("p.desc").alias("countryDesc"), col("a.elev").alias("elev"))
				.groupBy(col("continent"), col("country"), col("countryDesc"))
				.agg(sort_array(collect_list(col("elev"))).alias("elevations"))
				.sort(col("continent"), col("country"), col("countryDesc"));
    }
}
