package ar.edu.itba.graph;

import static ar.edu.itba.graph.Queries.countriesElevationsAverage;
import static ar.edu.itba.graph.Queries.countriesElevationsQuery;
import static ar.edu.itba.graph.Queries.directFlightsQuery;
import static ar.edu.itba.graph.Queries.directFlightsQueryWithoutFilter;
import static ar.edu.itba.graph.Queries.oneStopFlightsQuery;
import static ar.edu.itba.graph.Queries.oneStopFlightsQueryWithoutFilter;
import static ar.edu.itba.graph.TpeUtils.loadEdges;
import static ar.edu.itba.graph.TpeUtils.loadSchemaEdges;
import static ar.edu.itba.graph.TpeUtils.loadSchemaVertices;
import static ar.edu.itba.graph.TpeUtils.loadVertex;

import java.io.BufferedWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

import scala.collection.JavaConversions;

public class GraphFramesAppMain {

	public static void main(String[] args) throws ParseException, IOException {

		SparkConf spark = new SparkConf().setAppName("TP Final Fpannunzio");
		JavaSparkContext sparkContext = new JavaSparkContext(spark);
		SparkSession session = SparkSession.builder().appName("TP Final Fpannunzio").getOrCreate();
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(session);

		Graph auxGraph = new TinkerGraph();
		GraphMLReader reader = new GraphMLReader(auxGraph);

		final Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(conf);

		final Path path = new Path(args[0]);

		final Path parent = path.getParent();

		final FSDataInputStream is = fileSystem.open(path);

		reader.inputGraph(is);

		final List<Row> vertices = loadVertex(auxGraph.getVertices());
		final Dataset<Row> verticesDF = sqlContext.createDataFrame(vertices, loadSchemaVertices());

		final List<Row> edges = loadEdges(auxGraph.getEdges());
		final Dataset<Row> edgesDF = sqlContext.createDataFrame(edges, loadSchemaEdges());

		final GraphFrame myGraph = GraphFrame.apply(verticesDF, edgesDF);

		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
		// firstExercise(myGraph, fileSystem, timeStamp, parent);
		secondExercise(myGraph, fileSystem, timeStamp, parent);
		secondExerciseTest(myGraph, fileSystem, timeStamp, parent);

		sparkContext.close();

	}

	private static void firstExercise(final GraphFrame myGraph, final FileSystem fileSystem, final String timeStamp,
			final Path parent) throws IOException {

		final Dataset<Row> oneStop = oneStopFlightsQuery(myGraph);

		final Dataset<Row> oneStopVertex = oneStop.select("airportCode", "latitude", "longitude", "stepAirportCode", "destAirportCode");

		final Dataset<Row> direct = directFlightsQuery(myGraph);

		final Dataset<Row> directVertex = direct.select("airportCode", "latitude", "longitude", "destAirportCode");

		oneStopVertex.show(1000);
		directVertex.show();

		final BufferedWriter br = TpeUtils.getBufferedWriter(fileSystem, parent, timeStamp + "-b1.txt");

		br.write("One step\n");
		br.write("\n");
		
		br.write("AirportCode \t Latitude \t Longitude \t Travel\n");
		for (final Row row : oneStopVertex.collectAsList()) {
			br.write(row.getAs("airportCode").toString() + "\t" 
				+ row.getAs("latitude").toString() + "\t" 
				+ row.getAs("longitude").toString() + "\t" 
				+ "[" 
				+ row.getAs("airportCode").toString() + ", " 
				+ row.getAs("stepAirportCode").toString() + ", "
				+ row.getAs("destAirportCode").toString() 
				+ "]\n"
				);

		}
		br.write("\n");
		br.write("Direct\n");
		br.write("\n");
		br.write("AirportCode \t Latitude \t Longitude \t Travel\n");
		for (final Row row : directVertex.collectAsList()) {
			br.write(row.getAs("airportCode").toString() + "\t" 
				+ row.getAs("latitude").toString() + "\t" 
				+ row.getAs("longitude").toString() + "\t" 
				+ "[" 
				+ row.getAs("airportCode").toString() + ", " 
				+ row.getAs("destAirportCode").toString() 
				+ "]\n"
				);
		}
		br.close();
	}

	private static void firstExerciseTest(final GraphFrame myGraph, final FileSystem fileSystem, final String timeStamp,
			final Path parent) throws IOException {

		final Dataset<Row> oneStop = oneStopFlightsQuery(myGraph);

		final Dataset<Row> oneStopVertex = oneStop.select("airportCode", "latitude", "longitude", "stepAirportCode", "destAirportCode");

		final Dataset<Row> direct = directFlightsQueryWithoutFilter(myGraph);

		final Dataset<Row> directVertex = direct.select("airportCode", "latitude", "longitude", "destAirportCode");

		oneStopVertex.show(1000);
		directVertex.show();

		final BufferedWriter br = TpeUtils.getBufferedWriter(fileSystem, parent, timeStamp + "-b1.txt");

		br.write("One step\n");
		br.write("\n");
		
		br.write("AirportCode \t Latitude \t Longitude \t Travel\n");
		for (final Row row : oneStopVertex.collectAsList()) {
			br.write(row.getAs("airportCode").toString() + "\t" 
				+ row.getAs("latitude").toString() + "\t" 
				+ row.getAs("longitude").toString() + "\t" 
				+ "[" 
				+ row.getAs("airportCode").toString() + ", " 
				+ row.getAs("stepAirportCode").toString() + ", "
				+ row.getAs("destAirportCode").toString() 
				+ "]\n"
				);

		}
		br.write("\n");
		br.write("Direct\n");
		br.write("\n");
		br.write("AirportCode \t Latitude \t Longitude \t Travel\n");
		for (final Row row : directVertex.collectAsList()) {
			br.write(row.getAs("airportCode").toString() + "\t" 
				+ row.getAs("latitude").toString() + "\t" 
				+ row.getAs("longitude").toString() + "\t" 
				+ "[" 
				+ row.getAs("airportCode").toString() + ", " 
				+ row.getAs("destAirportCode").toString() 
				+ "]\n"
				);
		}
		br.close();
	}

	private static void secondExercise(final GraphFrame myGraph, final FileSystem fileSystem, final String timeStamp,
			final Path parent) throws IOException {

		final Dataset<Row> result = countriesElevationsQuery(myGraph);

		result.show(1000);
		result.printSchema();

		final Dataset<Row> resultVertex = result.select("continent", "country", "countryDesc", "elevations");
		
		final BufferedWriter br = TpeUtils.getBufferedWriter(fileSystem, parent, timeStamp + "-b2.txt");
		
		br.write("Countries elevations\n");
		br.write("\n");
		br.write("Continent \t Country \t CountryDescription \t Elevations\n");		
		for (final Row row : resultVertex.collectAsList()) {
			br.write(row.getAs("continent").toString() + "\t" 
				+ row.getAs("country").toString() + "\t" 
				+ row.getAs("countryDesc").toString() + "\t" 
				+ JavaConversions.asJavaCollection(row.getAs("elevations")).toString() + "\n"
				);
		}
		br.close();

		myGraph.vertices().printSchema();
		myGraph.edges().printSchema();
	}

	private static void secondExerciseTest(final GraphFrame myGraph, final FileSystem fileSystem, final String timeStamp,
			final Path parent) throws IOException {

		final Dataset<Row> result = countriesElevationsAverage(myGraph);

		result.show(1000);
		result.printSchema();

		final Dataset<Row> resultVertex = result.select("continent", "country", "countryDesc", "elevationsAvg");
		
		final BufferedWriter br = TpeUtils.getBufferedWriter(fileSystem, parent, timeStamp + "-b22.txt");
		
		br.write("Countries elevations\n");
		br.write("\n");
		br.write("Continent \t Country \t CountryDescription \t ElevationsAvg\n");		
		for (final Row row : resultVertex.collectAsList()) {
			br.write(row.getAs("continent").toString() + "\t" 
				+ row.getAs("country").toString() + "\t" 
				+ row.getAs("countryDesc").toString() + "\t" 
				+ row.getAs("elevationsAvg").toString() + "\n"
				);
		}
		br.close();

		myGraph.vertices().printSchema();
		myGraph.edges().printSchema();
	}
}
