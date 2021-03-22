package cn.hxx.recommender

import java.net.InetAddress

import cn.hxx.recommender2
import cn.hxx.recommender2.{Movie, Rating, Tag}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

/* 三个数据集的样例类 */
case class Movie(mid:Int, name:String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors:String)
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

/**
 *
 * @param uri 连接
 * @param db  数据库
 */
case class MongoConfig(uri:String, db:String)

/**
 *
 * @param httpHosts       http 主机列表，逗号分割
 * @param transportHosts  transport主机列表
 * @param index           需要操作的索引
 * @param clustername     集群名称 es-cluster
 */
case class ESConfig(httpHosts:String, transportHosts:String, index:String, clustername:String)

object DataLoader  {

    //定义常量
    val MOVIE_DATA_PATH = "D:\\Scala_workspace\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
    val RATING_DATA_PAHT = "D:\\Scala_workspace\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
    val TAGS_DATA_PAHT = "D:\\Scala_workspace\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

    val MONGODB_MOVIE_COLLECTION = "Movie"
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_TAG_COLLECTION = "Tag"
    val ES_MOVIE_INDEX = "Movie"

    def main(args: Array[String]): Unit = {

        val config = Map(
            "spark-cores" -> "local[*]",
            "mongo.uri" -> "mongodb://myCent:27017/recommender",
            "mongo.db" -> "recommender",
            "es.httpHosts" -> "myCent:9200",
            "es.transportHosts" -> "myCent:9300",
            "es.index" -> "recommender",
            "es.cluster.name" -> "es-cluster" //**********错在这
        )

        // 建立Spark连接
        val sparkConf = new SparkConf().setMaster(config("spark-cores")).setAppName("DataLoader")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._


        /*RDD => DataFrame*/
        val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
        val movieDF = movieRDD.map(
            items => {
                val attributes = items.split("\\^")
                Movie(attributes(0).toInt, attributes(1).trim, attributes(2).trim, attributes(3).trim, attributes(4).trim, attributes(5).trim, attributes(6).trim, attributes(7).trim, attributes(8).trim, attributes(9).trim)
            }
        ).toDF()

        val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PAHT)
        val ratingDF = ratingRDD.map(
            items => {
                val attributes = items.split(",")
                Rating(attributes(0).toInt, attributes(1).toInt, attributes(2).toDouble, attributes(3).toInt)
            }
        ).toDF()

        val tagRDD = spark.sparkContext.textFile(TAGS_DATA_PAHT)
        val tagDF = tagRDD.map(
            items => {
                val attributes = items.split(",")
                Tag(attributes(0).toInt,attributes(1).toInt,attributes(2).trim,attributes(3).toInt)
            }
        ).toDF()


        //封装MongoDB配置，隐式转换   TODO ****这里不懂
        implicit val mongoConfig = recommender2.MongoConfig(config("mongo.uri"),config("mongo.db"))

        //将数据保存到MongoDB
        storeDataInMongoDB(movieDF, ratingDF, tagDF)

        //(es)数据预处理,把movie对应的tag信息添加进去
        /**
         * mid, tags
         * 其中tags => tag1|tag2|tag3..
         */
        import org.apache.spark.sql.functions._
        val newTag = tagDF.groupBy($"mid")
                        .agg(concat_ws("|", collect_set($"tag")).as("tags"))
                        .select("mid","tags")

        //newTag和movie做左外连接，相同属性列mid
        val movieWithTagsDF = movieDF.join(newTag, Seq("mid"), "left")

        //将数据保存到ES
        implicit val esConfig = recommender2.ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))
        storeDataInES(movieWithTagsDF)

        //关闭Spark连接
        spark.stop()
    }


    //写入MongoDB函数
    def storeDataInMongoDB(movieDF:DataFrame, ratingDF:DataFrame, tagDF:DataFrame)(implicit mongoConfig: recommender2.MongoConfig): Unit ={
        //新建mongodb连接
        val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

        //如果已存在对应数据库，则先删除
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

        //把movieDF写入表中 - write.save()方法
        movieDF.write
                .option("uri",mongoConfig.uri)
                .option("collection",MONGODB_MOVIE_COLLECTION)  //集合名
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()

        //把ratingDF写入表中
        ratingDF.write
                .option("uri",mongoConfig.uri)
                .option("collection",MONGODB_RATING_COLLECTION)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()

        //把tagDF写入表中
        tagDF.write
                .option("uri",mongoConfig.uri)
                .option("collection",MONGODB_TAG_COLLECTION)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()

        //建立索引
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

        //关闭连接
        mongoClient.close()

    }


    //写入ES函数
    /**
     * 调试方法：
     *    1. http://mycent:9200/recommender/_search?pretty
     *    2. curl "myCent:9200/recommender/_search?pretty" -d'{"query":{"bool":{"must":{"exists":{"field":"tags"}}}}}'
     *    3. "hits"里的total是结果数
     */
    def storeDataInES(movieWithTagsDF: DataFrame)(implicit eSConfig: recommender2.ESConfig) : Unit ={
        //新建es配置
        val settings:Settings = Settings.builder()
                .put("cluster.name",eSConfig.clustername)
                .build()

        //新建一个es客户端
        val esClient = new PreBuiltTransportClient(settings)

        //正则，判断host格式
        /**
         * . 表示任意字符  + 表示任意数量 d 表示Int
         * (.+) 表示前半部分任意字符 => myCent
         * (\\d+) 表示后半部分任意数量Int => 9200
         */
        val REGEX_HOST_PORT = "(.+):(\\d+)".r

        //对多个用逗号分割的Host进行处理
        eSConfig.transportHosts.split(",").foreach{
            case REGEX_HOST_PORT(host: String, port:String) => {
                esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
            }
        }

        //清理遗留数据
        if(esClient.admin().indices().exists(new IndicesExistsRequest(eSConfig.index))
            .actionGet().isExists){
            //如果存在该索引，则删除
            esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
        }

        //创建
        esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))

        //写入包含Tag的movieWithTags
        movieWithTagsDF.write
                .option("es.nodes",eSConfig.httpHosts)
                .option("es.http.timeout","100m")
                .option("es.mapping.id","mid")
                .mode("overwrite")
                .format("org.elasticsearch.spark.sql")
                .save(eSConfig.index + "/" + ES_MOVIE_INDEX)

    }
}
