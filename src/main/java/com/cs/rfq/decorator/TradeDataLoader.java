package com.cs.rfq.decorator;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.types.DataTypes.*;

public class TradeDataLoader {

    private final static Logger log = LoggerFactory.getLogger(TradeDataLoader.class);

    /*StructType trade_schema = new StructType()
            .add("TraderID", LongType, true )
            .add("EntityID", LongType, true  )
            //.add("MsgType", IntegerType, true )
            //.add("TradeReportID", LongType, true)
            //.add("PreviouslyReported", StringType, true)
            .add("SecurityID", StringType, true)
            //.add("SecurityIDSource", StringType, true)
            .add("LastQty", LongType, true)
            .add("LastPx", DoubleType, true)
            .add("TradeDate", DateType, true)
            //.add("TransactTime", StringType, true)
            //.add("NoSides", IntegerType, true)
            //.add("Side", IntegerType, true)
            //.add("OrderID", LongType, true)
            .add("Currency", StringType, true);
 */


    public Dataset<Row> loadTrades(SparkSession session, String path) {
        //TODO: create an explicit schema for the trade data in the JSON files
        StructType schema = new StructType(new StructField[]{
                new StructField("TraderID", LongType, false, Metadata.empty()),
                new StructField("EntityID", LongType, false, Metadata.empty()),
                new StructField("SecurityID", StringType, false, Metadata.empty()),
                new StructField("LastQty", LongType, false, Metadata.empty()),
                new StructField("LastPx", DoubleType, false, Metadata.empty()),
                new StructField("TradeDate", DateType, false, Metadata.empty()),
                new StructField("Currency", StringType, false, Metadata.empty()),

        });

        //TODO: load the trades dataset
        Dataset<Row> trades = session.read().schema(schema).json(path);

        //TODO: log a message indicating number of records loaded and the schema used
        System.out.println("Number of trades loaded = " + trades.count());
        //trades.printSchema();

        return trades;
    }

}
