package cn.hxx.recommender2

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

/* 四个数据集的样例类 */
//豆瓣电影数据集
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 year: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: String)

case class Tag(mid: Int, tag: String)

case class Comment(uid: Int, mid: Int, content: String, timestamp: String)

/**
 *
 * @param uri 连接
 * @param db  数据库
 */
case class MongoConfig(uri: String, db: String)

/**
 *
 * @param httpHosts      http 主机列表，逗号分割
 * @param transportHosts transport主机列表
 * @param index          需要操作的索引
 * @param clustername    集群名称 es-cluster
 */
case class ESConfig(httpHosts: String, transportHosts: String, index: String, clustername: String)

object DataLoader2 {

    //定义常量
    val MOVIE_DATA_PATH = "D:\\Scala_workspace\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\douban\\movies.csv"
    val COMMENTS_DATA_PATH = "D:\\Scala_workspace\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\douban\\comments.csv"

    val MONGODB_MOVIE_COLLECTION = "Movie"
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_TAG_COLLECTION = "Tag"
    val MONGODB_COMMENT_COLLECTION = "Comment"
    val ES_MOVIE_INDEX = "Movie"

    def main(args: Array[String]): Unit = {

        val config = Map(
            "spark-cores" -> "local[*]",
            "mongo.uri" -> "mongodb://myCent:27017/recommender2",
            "mongo.db" -> "recommender2",
            "es.httpHosts" -> "myCent:9200",
            "es.transportHosts" -> "myCent:9300",
            "es.index" -> "recommender2",
            "es.cluster.name" -> "es-cluster"
        )

        // 建立Spark连接
        val sparkConf = new SparkConf().setMaster(config("spark-cores")).setAppName("DataLoader2")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._


        /*RDD => DataFrame*/
        //case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
        //                 year: Int, language: String, genres: String, actors: String, directors: String)
        val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
        val header1 = movieRDD.first()
        val movieDF = movieRDD.filter(row => row != header1) //过滤掉第一行
                .map(
                    items => {
                        val attributes = items.split(",").map {  // 每个数据都自带" ", 所以做个过滤
                            str =>
                                    if(str.length > 2){
                                        val end = str.length - 1
                                        str.substring(1, end)
                                    }
                                    else ""

                        }
                        Movie(attributes(0).toInt, attributes(1).trim, attributes(16).trim, attributes(11).trim, attributes(14).trim, attributes(18).trim, attributes(10).trim, attributes(8).trim, attributes(3).trim, attributes(5).trim)
                    }
                ).toDF()
        movieDF.rdd.take(10).foreach(println)
        //case class Rating(uid: String, mid: Int, score: Double, timestamp: String)
        val ratingRDD = spark.sparkContext.textFile(COMMENTS_DATA_PATH)
        val header2 = ratingRDD.first()
        val ratingDF = ratingRDD.filter(row => row != header2) //过滤掉第一行
                .filter{
                    items =>
                        val attributes = items.split(",").map {
                            str =>
                                if(str.length > 2){
                                    val end = str.length - 1
                                    str.substring(1, end)
                                }
                                else ""
                        }
                        attributes.length == 14 && attributes(9) != ""
                }
                .map(
                    items => {
                        val attributes = items.split(",").map {
                            str =>
                                if(str.length > 2){
                                    val end = str.length - 1
                                    str.substring(1, end)
                                }
                                else ""
                        }
                        Rating(attributes(8).toInt, attributes(4).toInt, attributes(9).toDouble, attributes(0).trim)
                    }
                ).toDF()

        //case class Comment(uid: String, mid: Int, content: String, timestamp: String)
        val commentRDD = spark.sparkContext.textFile(COMMENTS_DATA_PATH)
        val header3 = commentRDD.first()
        val commentDF = commentRDD.filter(row => row != header3).filter(_.split(",").length == 14)
                .map(
                    items => {
                        val attributes = items.split(",").map {
                            str =>
                                if(str.length > 2){
                                    val end = str.length - 1
                                    str.substring(1, end)
                                }
                                else ""
                        }
                        Comment(attributes(8).toInt, attributes(4).toInt, attributes(1).trim, attributes(0).trim)
                    }
                ).toDF()

        //case class Tag(mid: Int, tag: String)
        val tagRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
        val header4 = tagRDD.first()
        val tagDF = tagRDD.filter(row => row != header4)
                .map(
                    items => {
                        val attributes = items.split(",").map {
                            str =>
                                if(str.length > 2){
                                    val end = str.length - 1
                                    str.substring(1, end)
                                }
                                else ""
                        }
                        Tag(attributes(0).toInt, attributes(17).trim)
                    }
                ).toDF()


        //封装MongoDB配置，隐式转换   TODO ****这里不懂
        implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

        //将数据保存到MongoDB
        storeDataInMongoDB(movieDF, ratingDF, tagDF, commentDF)

        //(es)数据预处理,把movie对应的tag信息添加进去
        /**
         * mid, tags
         * 其中tags => tag1/tag2/tag3..
         */
//        import org.apache.spark.sql.functions._
//        val newTag = tagDF.groupBy($"mid")
//                .agg(concat_ws("/", collect_set($"tag")).as("tags"))
//                .select("mid", "tags")
//
//        //newTag和movie做左外连接，相同属性列mid
//        val movieWithTagsDF = movieDF.join(newTag, Seq("mid"), "left")

        //将数据保存到ES
        implicit val esConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))
        storeDataInES(tagDF)

        //关闭Spark连接
        spark.stop()
    }


    //写入MongoDB函数
    def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame, commentDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
        //新建mongodb连接
        val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

        //如果已存在对应数据库，则先删除
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_COMMENT_COLLECTION).dropCollection()

        //把movieDF写入表中 - write.save()方法
        movieDF.write
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_MOVIE_COLLECTION) //集合名
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()

        //把ratingDF写入表中
        ratingDF.write
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_RATING_COLLECTION)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()

        //把tagDF写入表中
        tagDF.write
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_TAG_COLLECTION)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()

        //把CommentDF写入表中
        commentDF.write
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_COMMENT_COLLECTION)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()

        //建立索引
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_COMMENT_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
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
    def storeDataInES(movieWithTagsDF: DataFrame)(implicit eSConfig: ESConfig): Unit = {
        //新建es配置
        val settings: Settings = Settings.builder()
                .put("cluster.name", eSConfig.clustername)
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
        eSConfig.transportHosts.split(",").foreach {
            case REGEX_HOST_PORT(host: String, port: String) => {
                esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
            }
        }

        //清理遗留数据
        if (esClient.admin().indices().exists(new IndicesExistsRequest(eSConfig.index))
                .actionGet().isExists) {
            //如果存在该索引，则删除
            esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
        }

        //创建
        esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))

        //写入包含Tag的movieWithTags
        movieWithTagsDF.write
                .option("es.nodes", eSConfig.httpHosts)
                .option("es.http.timeout", "100m")
                .option("es.mapping.id", "mid")
                .mode("overwrite")
                .format("org.elasticsearch.spark.sql")
                .save(eSConfig.index + "/" + ES_MOVIE_INDEX)

    }
}
