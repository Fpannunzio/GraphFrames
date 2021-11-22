package ar.edu.itba.graph;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import ar.edu.itba.graph.graphProperties.EdgePropertiesEnum;
import ar.edu.itba.graph.graphProperties.VertexPropertiesEnum;

public class TpeUtils {

    public static StructType loadSchemaVertices() {
		// metadata
		final List<StructField> vertFields = Arrays.stream(VertexPropertiesEnum.values())
			.map(VertexPropertiesEnum::toStructField)
			.collect(Collectors.toList());

		vertFields.add(0, DataTypes.createStructField("id",  DataTypes.LongType, false));
		
		return DataTypes.createStructType(vertFields);
	}

	public static StructType loadSchemaEdges() {
		// metadata
		final List<StructField> edgeFields = Arrays.stream(EdgePropertiesEnum.values())
			.map(EdgePropertiesEnum::toStructField)
			.collect(Collectors.toList());

		edgeFields.add(0, DataTypes.createStructField("id", DataTypes.LongType, false));
        edgeFields.add(1, DataTypes.createStructField("src", DataTypes.LongType, false));
        edgeFields.add(2, DataTypes.createStructField("dst", DataTypes.LongType, false));

		return DataTypes.createStructType(edgeFields);
	}

	public static List<Row> loadVertex(final Iterable<Vertex> vIterator) {
		final List<Row> vertexList = new ArrayList<Row>();
		final List<String> identifiers = Arrays.stream(VertexPropertiesEnum.values())
			.map(VertexPropertiesEnum::getIdentifier)
			.collect(Collectors.toList());	

		for (final Vertex vertex : vIterator) {
		  final List<Object> properties = identifiers.stream().map(identifier -> vertex.getProperty(identifier)).collect(Collectors.toList());
		  properties.add(0, Long.valueOf((String) vertex.getId()));
		  vertexList.add(RowFactory.create(properties.toArray(new Object[0])));
		}

		return vertexList;
	}

	public static List<Row> loadEdges(final Iterable<Edge> eIterator) {
		final List<Row> edgeList = new ArrayList<Row>();
		final List<String> identifiers = Arrays.stream(EdgePropertiesEnum.values())
			.map(EdgePropertiesEnum::getIdentifier)
			.collect(Collectors.toList());	
		
		for (final Edge edge : eIterator) {
		  final List<Object> properties = identifiers.stream().map(identifier -> edge.getProperty(identifier)).collect(Collectors.toList());
		  properties.add(0, Long.valueOf((String) edge.getId()));
		  properties.add(1, Long.valueOf((String) edge.getVertex(Direction.OUT).getId()));
		  properties.add(2, Long.valueOf((String) edge.getVertex(Direction.IN).getId()));
		  edgeList.add(RowFactory.create(properties.toArray(new Object[0])));
		}

		return edgeList;
	}

	public static BufferedWriter getBufferedWriter(final FileSystem fileSystem, final Path parent, final String filePath) throws IOException {
		final Path outPath = new Path(parent, filePath);
		if (fileSystem.exists(outPath)) {
			fileSystem.delete(outPath, true);
		}

		final OutputStream os = fileSystem.create(outPath);
		return new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
	}
}
