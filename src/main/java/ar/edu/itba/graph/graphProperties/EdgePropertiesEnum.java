package ar.edu.itba.graph.graphProperties;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

public enum EdgePropertiesEnum {
    LABELE("labelE", DataTypes.StringType, false),
    DIST("dist", DataTypes.IntegerType, true),  
    ;

    private String identifier;
    private DataType dataType;
    private boolean isNullable;

    private EdgePropertiesEnum(String identifier, DataType dataType, boolean isNullable) {
        this.identifier = identifier;
        this.dataType = dataType;
        this.isNullable = isNullable;
    }

    public String getIdentifier(){
        return identifier;
    }

    public StructField toStructField() {
        return DataTypes.createStructField(identifier, dataType, isNullable);
    }
    
}
