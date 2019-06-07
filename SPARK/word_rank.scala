// sposob uruchomienia
// spark-shell -i word_rank.scala --conf spark.driver.args="woodworking"
// 
//
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

val hc = new org.apache.spark.sql.hive.HiveContext(sc);
val args = sc.getConf.get("spark.driver.args").split("\\s+");
val forumName = args(0);
val sqlRDD = spark.sql("SELECT cleansed_body FROM stage.posts_cleansed").map(x=>x.toString()).rdd.flatMap(line => line.replaceAll("[`*{}()>#+:~'%^&@\\[\\]<?;,\"!$=|.]", "").split(" ")).map(word => (word, 1)).reduceByKey(_ + _);
val sqlDF = sqlRDD.toDF().withColumnRenamed("_1","word").withColumnRenamed("_2","rank").orderBy(desc("rank"));
sqlDF.registerTempTable("word_ranking_temp_table");

hc.sql("alter table AGGR.POST_BODY_WORDS_CURRENT_RANKING drop if exists partition (forum_name='" + forumName + "')");
hc.sql("set hive.exec.dynamic.partition.mode=nonstrict");
hc.sql("insert into AGGR.POST_BODY_WORDS_CURRENT_RANKING PARTITION (forum_name)  select current_date, word, use_count, use_rank_value, load_timestamp, forum_name from  (SELECT current_date, word, rank as use_count, row_number() over (order by rank desc) as use_rank_value, current_timestamp as load_timestamp, '" + forumName + "' as forum_name from word_ranking_temp_table) a where use_rank_value<101");
System.exit(0);