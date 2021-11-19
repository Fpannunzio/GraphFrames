package ar.edu.itba.graph;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
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
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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

        final FSDataInputStream is = fileSystem.open(path);

        reader.inputGraph(is);

		// for (Edge edge : auxGraph.getEdges()) {
		// 	System.out.println("Edge " + edge.getId() + " goes from " + edge.getVertex(Direction.OUT).getId() + " to " + edge.getVertex(Direction.IN).getId());
		// }
	 
		// Iterable<Vertex> vertices = auxGraph.getVertices();
    	// Iterator<Vertex> verticesIterator = vertices.iterator();
		
	 
		// while (verticesIterator.hasNext()) {
	 
		//   Vertex vertex = verticesIterator.next();
		  
		//   System.out.println("Vertex index = " + vertex.getId());
		// }

		List<Row> vertices = loadVertex(auxGraph.getVertices());
		Dataset<Row> verticesDF = sqlContext.createDataFrame(vertices, loadSchemaVertices());
		

		List<Row> edges = loadEdges(auxGraph.getEdges());
		Dataset<Row> edgesDF = sqlContext.createDataFrame(edges, loadSchemaEdges());

		GraphFrame myGraph = GraphFrame.apply(verticesDF, edgesDF);

		Dataset<Row> oneStop = myGraph
			.filterVertices("labelV = 'airport'")
			.filterEdges("labelE = 'route'")
			.find("(a)-[e]->(b); (b)-[e2]->(c)")
			.filter("a.lat < 0 and a.lon < 0")
			.filter("c.code = 'SEA'")
			.filter("a.id != b.id and a.id != c.id and b.id != c.id");

		Dataset<Row> oneStopVertex = oneStop.select("a.code", "a.lat", "a.lon", "b.code", "c.code");
		
		Dataset<Row> direct = myGraph
			.filterVertices("labelV = 'airport'")
			.filterEdges("labelE = 'route'")
			.find("(a)-[e]->(b)")
			.filter("a.lat < 0 and a.lon < 0")
			.filter("b.code = 'SEA'")
			.filter("a.id != b.id");
		
		Dataset<Row> directVertex = direct.select("a.code", "a.lat", "a.lon", "b.code");	
			
		System.out.println("OneStepVertex: " + oneStopVertex.collectAsList().size() + " Direct: " + directVertex.collectAsList().size());
		// directVertex.collectAsList().stream().forEach(row -> System.out.println(row.toString()));
	
		// oneStopVertex.collectAsList().stream().forEach(row -> System.out.println(row.toString()));
			
		sparkContext.close();

	}


	public static StructType loadSchemaVertices() {
		// metadata
		List<StructField> vertFields = Arrays.stream(VertexPropertiesEnum.values()).map(VertexPropertiesEnum::toStructField).collect(Collectors.toList());
		vertFields.add(0, DataTypes.createStructField("id",  DataTypes.LongType, false));
		StructType schema = DataTypes.createStructType(vertFields);

		return schema;
	}

	public static StructType loadSchemaEdges() {
		// metadata
		List<StructField> edgeFields = Arrays.stream(EdgePropertiesEnum.values()).map(EdgePropertiesEnum::toStructField).collect(Collectors.toList());
		edgeFields.add(0, DataTypes.createStructField("id", DataTypes.LongType, false));
        edgeFields.add(1, DataTypes.createStructField("src", DataTypes.LongType, false));
        edgeFields.add(2, DataTypes.createStructField("dst", DataTypes.LongType, false));
		StructType schema = DataTypes.createStructType(edgeFields);

		return schema;
	}

	public static List<Row> loadVertex(final Iterable<Vertex> vIterator) {
		final List<Row> vertexList = new ArrayList<Row>();
		final List<String> identifiers = Arrays.stream(VertexPropertiesEnum.values()).map(VertexPropertiesEnum::getIdentifier).collect(Collectors.toList());	
		for (final Vertex vertex : vIterator) {
		  final List<Object> properties = identifiers.stream().map(identifier -> vertex.getProperty(identifier)).collect(Collectors.toList());
		  properties.add(0, Long.valueOf((String) vertex.getId()));
		  vertexList.add(RowFactory.create(properties.toArray(new Object[0])));
		}

		return vertexList;
	}

	public static List<Row> loadEdges(final Iterable<Edge> eIterator) {
		final List<Row> edgeList = new ArrayList<Row>();
		
		for (final Edge edge : eIterator) {
		  final List<Object> properties = edge.getPropertyKeys().stream().map(key -> edge.getProperty(key)).collect(Collectors.toList());
		  properties.add(0, Long.valueOf((String) edge.getId()));
		  properties.add(1, Long.valueOf((String) edge.getVertex(Direction.OUT).getId()));
		  properties.add(2, Long.valueOf((String) edge.getVertex(Direction.IN).getId()));
		  edgeList.add(RowFactory.create(properties.toArray(new Object[0])));
		}

		return edgeList;
	}
}