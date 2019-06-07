val args = sc.getConf.get("spark.driver.args").split("\\s+");
val forumName = args(0);
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
var df4, df5, df6 = sqlContext.emptyDataFrame;
var rawRecordsInfoDF, 
    stageRecordsInfoDF, 
    mainRecordsInfoDF ,
    singleTableInfoDF = sqlContext.emptyDataFrame;
var dfAllCountStats = sqlContext.emptyDataFrame;

val stageToMainMapping = Map("badges" -> "d_badge"
                            ,"calendar"->"d_calendar"
                            ,"closereason"->"d_close_reason"
                            ,"comments"->"f_comment"
                            ,"country"->"d_country"
                            ,"posthistory"->"f_post_history"
                            ,"posthistorytype"->"d_post_hist_type"
                            ,"postlinks"->"f_post_link"
                            ,"postlinktype"->"d_post_link_type"
                            ,"posts"->"f_post"
                            ,"tags"->"d_tag"
                            ,"users"->"d_user"
                            ,"votes"->"d_vote"
                            ,"votetype"->"d_vote_type");

var i = 0;
val hc = new org.apache.spark.sql.hive.HiveContext(sc)
hc.sql("use raw");
val rawDatabaseTablesListDF = hc.sql("show tables").filter("isTemporary==false and database='raw'");

dfAllCountStats = dfAllCountStats.withColumn("FORUM_NAME",lit("")).withColumn("RAW_TABLE_NAME",lit("")).withColumn("RAW_RECS",lit("")) .withColumn("STAGE_TABLE_NAME",lit("")).withColumn("STAGE_RECS",lit("")).withColumn("MAIN_TABLE_NAME",lit("")).withColumn("REAL_MAIN_TABLE_NAME",lit("")).withColumn("MAIN_RECS",lit("")).withColumn("STAGE_LOAD_STATUS",lit("")).withColumn("MAIN_LOAD_STATUS",lit(""));

val tableNamesList = rawDatabaseTablesListDF.select("tableName").map(r => r.getString(0)).collect.toList;

//println("aaa");

for (i <-0 until tableNamesList.length)
{
    if ( tableNamesList(i)!="nposts") 
    {
     df4 = hc.sql("select * from raw." + tableNamesList(i) );
     df5 = hc.sql("select * from stage." + tableNamesList(i) );
     df6 = hc.sql("select m.*,'" + stageToMainMapping(tableNamesList(i)) +"' as REAL_MAIN_TABLE_NAME  from main." + stageToMainMapping(tableNamesList(i)) + " m");
   
     rawRecordsInfoDF = df4.withColumn("FORUM_NAME",lit(forumName)).withColumn("RAW_TABLE_NAME",lit(tableNamesList(i))).groupBy("FORUM_NAME","RAW_TABLE_NAME").count().withColumnRenamed("count","RAW_RECS")
     if (List("calendar","closereason","country","posthistorytype","postlinktype","votetype") contains tableNamesList(i)) 
     {
      stageRecordsInfoDF = df5.withColumn("STAGE_TABLE_NAME",lit(tableNamesList(i))).groupBy("STAGE_TABLE_NAME").count().withColumnRenamed("count","STAGE_RECS")
      mainRecordsInfoDF = df6.withColumn("MAIN_TABLE_NAME",lit(tableNamesList(i))).groupBy("MAIN_TABLE_NAME","REAL_MAIN_TABLE_NAME").count().withColumnRenamed("count","MAIN_RECS")
     }
     else
     {
      stageRecordsInfoDF = df5.withColumn("STAGE_TABLE_NAME",lit(tableNamesList(i))).filter("forum_name='"+ forumName +"'").groupBy("STAGE_TABLE_NAME").count().withColumnRenamed("count","STAGE_RECS")
      mainRecordsInfoDF = df6.withColumn("MAIN_TABLE_NAME",lit(tableNamesList(i))).filter("forum_name='"+ forumName +"'").groupBy("MAIN_TABLE_NAME","REAL_MAIN_TABLE_NAME").count().withColumnRenamed("count","MAIN_RECS")
     }
     ;
     
     singleTableInfoDF=  rawRecordsInfoDF.select(col("FORUM_NAME"), col("RAW_TABLE_NAME"),col("RAW_RECS"))
                     .join(stageRecordsInfoDF, rawRecordsInfoDF("RAW_TABLE_NAME") === stageRecordsInfoDF("STAGE_TABLE_NAME"), "left_outer")  
                     .join(mainRecordsInfoDF, mainRecordsInfoDF("MAIN_TABLE_NAME") === stageRecordsInfoDF("STAGE_TABLE_NAME"), "left_outer");  
    
    singleTableInfoDF = singleTableInfoDF.withColumn("STAGE_LOAD_STATUS",when(col("RAW_RECS") === col("STAGE_RECS"),"ALL_RECS_LOADED")).withColumn("MAIN_LOAD_STATUS",when(col("STAGE_RECS") === col("MAIN_RECS"),"ALL_RECS_LOADED"));  
    
    dfAllCountStats = dfAllCountStats.union(singleTableInfoDF);                         
	 
        
    }
}
//singleTableInfoDF.show();

//dfAllCountStats.show();
dfAllCountStats.registerTempTable("stat_temp_table");

hc.sql("alter table AGGR.TABLE_STATISTICS drop if exists partition (forum_name='" + forumName + "')");
hc.sql("set hive.exec.dynamic.partition.mode=nonstrict");
hc.sql("insert into AGGR.TABLE_STATISTICS PARTITION (forum_name) SELECT RAW_TABLE_NAME,STAGE_TABLE_NAME,REAL_MAIN_TABLE_NAME, RAW_RECS, STAGE_RECS, MAIN_RECS, CURRENT_TIMESTAMP, '" + forumName + "'  from stat_temp_table");

System.exit(0);