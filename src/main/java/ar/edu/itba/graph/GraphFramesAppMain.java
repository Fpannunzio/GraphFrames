package ar.edu.itba.graph;

import static ar.edu.itba.graph.TpeUtils.NEGATIVE_LATITUTE;
import static ar.edu.itba.graph.TpeUtils.NEGATIVE_LONGITUDE;
import static ar.edu.itba.graph.TpeUtils.loadEdges;
import static ar.edu.itba.graph.TpeUtils.loadSchemaEdges;
import static ar.edu.itba.graph.TpeUtils.loadSchemaVertices;
import static ar.edu.itba.graph.TpeUtils.loadVertex;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.sort_array;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

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
		firstExercise(myGraph, fileSystem, timeStamp, parent);
		secondExercise(myGraph, fileSystem, timeStamp, parent);

		sparkContext.close();

	}

	private static void firstExercise(final GraphFrame myGraph, final FileSystem fileSystem, final String timeStamp,
			final Path parent) throws IOException {
		final Dataset<Row> oneStop = myGraph.filterVertices("labelV = 'airport'").filterEdges("labelE = 'route'")
				.find("(a)-[e]->(b); (b)-[e2]->(c)")
				.filter(NEGATIVE_LATITUTE)
				.filter(NEGATIVE_LONGITUDE)
				.filter("c.code = 'SEA'").filter("a.id != b.id and a.id != c.id and b.id != c.id");

		final Dataset<Row> oneStopVertex = oneStop.select("a.code", "a.lat", "a.lon", "b.code", "c.code");

		final Dataset<Row> direct = myGraph.filterVertices("labelV = 'airport'").find("(a)-[e]->(b)")
				.filter(NEGATIVE_LATITUTE)
				.filter(NEGATIVE_LONGITUDE)
				.filter("b.code = 'SEA'").filter("a.id != b.id");

		final Dataset<Row> directVertex = direct.select("a.code", "a.lat", "a.lon", "b.code");

		oneStopVertex.show(1000);
		directVertex.show();

		final BufferedWriter br = TpeUtils.getBufferedWriter(fileSystem, parent, timeStamp + "-b1.txt");

		br.write("One step\n");
		br.write("\n");
		
		for (final Row row : oneStopVertex.collectAsList()) {
			br.write(row.toString());
			br.write("\n");
			// os.writeUTF(row.toString());
		}

		br.write("\n");
		br.write("Direct\n");
		br.write("\n");
		
		for (final Row row : directVertex.collectAsList()) {
			br.write(row.toString());
			// os.writeUTF(row.toString());
		}
		br.close();
	}

	private static void secondExercise(final GraphFrame myGraph, final FileSystem fileSystem, final String timeStamp,
			final Path parent) throws IOException {

		final Dataset<Row> result = myGraph.filterEdges("labelE = 'contains'").find("(c)-[]->(a); (p)-[]->(a)")
				.filter(col("a.labelV").eqNullSafe("airport")).filter(col("c.labelV").eqNullSafe("continent"))
				.filter(col("p.labelV").eqNullSafe("country"))
				.select(col("c.desc").alias("continent"), col("a.country").alias("country"),
						col("p.desc").alias("countryDesc"), col("a.elev").alias("elev"))
				.groupBy(col("continent"), col("country"), col("countryDesc"))
				.agg(sort_array(collect_list(col("elev"))).alias("elevations"))
				.sort(col("continent"), col("country"), col("countryDesc"));

		result.show(1000);
		result.printSchema();

		final Dataset<Row> resultVertex = result.select("continent", "country", "countryDesc", "elevations");
		
		final BufferedWriter br = TpeUtils.getBufferedWriter(fileSystem, parent, timeStamp + "-b2.txt");

		br.write("Countries elevations\n");
		br.write("\n");
		for (final Row row : resultVertex.collectAsList()) {
			br.write(row.toString());
			br.write("\n");
		}
		br.close();
	}
}
