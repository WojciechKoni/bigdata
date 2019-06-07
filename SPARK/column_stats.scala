import org.apache.spark.sql.types.StringType
val sqlContext = new org.apache.spark.sql.SQLContext(sc);
var finalDF, tableDF = sqlContext.emptyDataFrame;
var columnName = "forum_name";
var x = sqlContext.emptyDataFrame;
var finalDF = x.withColumn("forum_name", lit("").cast(StringType)).withColumn("TABLE_NAME", lit("").cast(StringType)).withColumn("COLUMN_NAME", lit("").cast(StringType)).withColumn("MIN_VALUE", lit("").cast(StringType)).withColumn("MAX_VALUE", lit("").cast(StringType)).withColumn("UNIQUE_VALUES", lit("").cast(StringType)).withColumn("COUNT", lit("").cast(StringType)).withColumn("NULLS", lit("").cast(StringType)).withColumn("AVG_VALUE", lit("").cast(StringType)) ;
var singleEntryDF = sqlContext.emptyDataFrame;
var minCol = min(columnName).alias("MIN_VALUE"); 
var maxCol = max(columnName).alias("MAX_VALUE");  
var uniqCol = countDistinct(columnName).alias("UNIQUE_VALUES");  
var avgCol = mean(columnName).alias("AVG_VALUE");   
var cntCol = count(columnName).alias("VALUES");  
var nullCntCol = sum(when(finalDF(columnName).isNull, lit(1)).otherwise(lit(0))).alias("NULLS");

var databaseIndex = 0;
var tableIndex = 0;
var columnIndex = 0;
var dataType = "";

val databases = List("STAGE" ,"MAIN","AGGR");
val hc = new org.apache.spark.sql.hive.HiveContext(sc);

//hc.sql("alter table AGGR.COLUMN_STATS drop if exists partition (forum_name='woodworking')");
//hc.sql("truncate table AGGR.COLUMN_STATS drop if exists partition (forum_name='woodworking')");
//hc.sql("set hive.exec.dynamic.partition.mode=nonstrict");

println(databases(0));

for (databaseIndex<-0 until databases.length)
{ 
   hc.sql("use "+databases(databaseIndex));
   var tables = hc.sql("show tables").filter("isTemporary==false and upper(database)='"+ databases(databaseIndex) +"'").select("tableName").map(r => r.getString(0)).collect().toList;
 
   
for (tableIndex<-0  until tables.length)
{ 
  println(databases(databaseIndex) + "." + tables(tableIndex));
 
 tableDF = hc.sql("select * from " + databases(databaseIndex) + "." + tables(tableIndex) );
 var columns = tableDF.columns;

// println("czy forum_name na liscie_kolumn="+columns.toList.contains("FORUM_NAME"));
//  println("czy forum_name na liscie_kolumn="+columns.toList.contains("forum_name"));
 
 for (columnIndex<-0 until columns.length){  
     columnName = columns(columnIndex);
     
     println(columnName);
     
     dataType  = tableDF.schema.fields(columnIndex).dataType.toString();
     minCol = min(columnName).alias("MIN_VALUE"); 
     maxCol = max(columnName).alias("MAX_VALUE");  
     uniqCol = countDistinct(columnName).alias("UNIQUE_VALUES");  
     cntCol = count(columnName).alias("COUNT");  
     nullCntCol = sum(when(tableDF(columnName).isNull, lit(1)).otherwise(lit(0))).alias("NULLS");
     if (List("TimestampType","StringType","DateType").contains(dataType)) {
         avgCol = lit("N/A").alias("AVG_VALUE");
     } else
     {
         avgCol = mean(columnName).alias("AVG_VALUE");
     };
     
     
     if (!columns.toList.contains("FORUM_NAME") && !columns.toList.contains("forum_name")) {
         singleEntryDF = tableDF.agg(minCol, maxCol, uniqCol, avgCol, cntCol, nullCntCol).withColumn("TABLE_NAME",lit(tables(tableIndex))).withColumn("COLUMN_NAME",lit(columns(columnIndex))).withColumn("forum_name",lit("forum_name")).select("forum_name","TABLE_NAME","COLUMN_NAME","MIN_VALUE","MAX_VALUE","UNIQUE_VALUES","COUNT","NULLS","AVG_VALUE"); 
                  
     } 
     else {
          singleEntryDF = tableDF.groupBy("forum_name").agg(minCol, maxCol, uniqCol, avgCol, cntCol, nullCntCol).withColumn("TABLE_NAME",lit(tables(tableIndex))).withColumn("COLUMN_NAME",lit(columns(columnIndex))).select("forum_name","TABLE_NAME","COLUMN_NAME","MIN_VALUE","MAX_VALUE","UNIQUE_VALUES","COUNT","NULLS","AVG_VALUE") ;    
     }
    
      //finalDF = finalDF.unionAll(singleEntryDF); 
      //finalDF.show();
      //finalDF
      singleEntryDF.registerTempTable("stats_temp_table");
      //println("final2");
      hc.sql("insert into AGGR.COLUMN_STATS  SELECT '"+databases(databaseIndex)+"', TABLE_NAME,COLUMN_NAME,MIN_VALUE,MAX_VALUE,UNIQUE_VALUES,COUNT,NULLS,AVG_VALUE, CURRENT_TIMESTAMP, forum_name  from stats_temp_table");
      println("next");
     
 }
 
   
 println("final");
//finalDF.registerTempTable("stats_temp_table");
//println("final2");
//hc.sql("insert into AGGR.COLUMN_STATS  SELECT '"+databases(databaseIndex)+"', TABLE_NAME,COLUMN_NAME,MIN_VALUE,MAX_VALUE,UNIQUE_VALUES,COUNT,NULLS,AVG_VALUE, CURRENT_TIMESTAMP, forum_name  from stats_temp_table");
//println("next");
  //finalDF.printSchema();
}

}
finalDF.show();
//PARTITION (forum_name)
System.exit(0);