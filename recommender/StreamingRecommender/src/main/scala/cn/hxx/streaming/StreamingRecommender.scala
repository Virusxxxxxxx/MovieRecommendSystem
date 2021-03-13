package cn.hxx.streaming

//kafka,flume,spark-streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

//定义一个推荐样例类
case class Recommendation(mid: Int, score: Double)

//定义基于ALS预测评分的用户推荐列表
case class UserRecs(uid: Int, recs: Seq[Recommendation])

//定义基于LFM电影特征向量的电影相似度列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

//MongoConfig样例类
case class MongoConfig(uri: String, db: String)

//数据库连接助手，序列化
object ConnHelper extends Serializable {
    //懒加载Redis和MongoDB配置
    lazy val jedis = new Jedis("myCent")
    lazy val mongoClient = MongoClient(MongoClientURI("mongodb://myCent:27017/recommender"))
}


object StreamingRecommender {
    val MAX_USER_RATINGS_NUM = 20 //选取用户的K次历史评分 K = 20
    val MAX_SIM_MOVIES_NUM = 20 //备选推荐电影数 K = 20
    val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs" //用户实时推荐表(区别于UserRecs表)
    val MONGODB_RATING_COLLECTION = "Rating" //用来过滤已看过的电影(一般不会推荐已看过的电影)
    val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs" //电影推荐列表

    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://mycent:27017/recommender",
            "mongo.db" -> "recommender",
            "kafka.topic" -> "recommender" //kafka.topic
        )

        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        //获取streaming context
        val sc = spark.sparkContext
        val ssc = new StreamingContext(sc, Seconds(2)) //第二个参数 => batch duration批处理时间，每一批次的处理时间

        import spark.implicits._
        implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

        // 加载电影相似度矩阵，并广播?出去
        val simMovieMatrix = spark.read
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[MovieRecs]
                .rdd
                .map { //为了查询相似度方便，转成map，通过key即可得到value
                    movieRecs =>
                        //注：这里不能直接recs.toMap, 因为后面是Seq类型，直接toMap会出错
                        (movieRecs.mid, movieRecs.recs.map(x => (x.mid, x.score)).toMap)
                }.collectAsMap()
        //广播
        //作用：由于相似电影推荐列表很大，用广播可以使每个
        // executer上会留存一份副本，而不需要每个任务保存一份，节省内存资源
        val simMovieMatrixBroadCast = sc.broadcast(simMovieMatrix)


        //定义kafka连接参数
        val kafkaParam = Map(
            "bootstrap.servers" -> "myCent:9092", //kafka集群
            "key.deserializer" -> classOf[StringDeserializer], //反序列化工具
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "recommender",
            "auto.offset.reset" -> "latest" //初始设置
        )

        //通过kafka创建一个DStream
        val kafkaDStream = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent, //偏向连续的策略
            //Subscribe(topic, kafka配置项)
            ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParam)
        )

        //把原始数据(UID|MID|SCORE|TIMESTAMP)转换成评分流
        val ratingStream = kafkaDStream.map {
            msg =>
                val attr = msg.value().split("\\|")
                (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
        }

        //流式处理，核心实时算法部分
        ratingStream.foreachRDD {
            rdds =>
                rdds.foreach {
                    case (uid, mid, score, timestamp) => {
                        println(">>>>>>>>>>>>>>>>>>>>>>> rating data coming >>>>>>>>>>>>>>>>>>>>>>>")
                        // step1: 从Redis里获取当前用户最近的K次评分，Array[(mid, score)]
                        val userRecentlyRating = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)

                        //step2: 从相似度矩阵中取出和当前电影最相似的N个电影，作为推荐备选列表，Array[mid]
                        val candidateMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)

                        //step3: 对每个备选电影，计算推荐优先级，得到实时推荐列表，Array[(mid, score)]

                        //step4: 把推荐数据保存到MongoDB
                    }
                }
        }

        //开始接受和处理数据
        ssc.start()

        println(">>>>>>>>>>>>>>>>>>>>>>> streaming started >>>>>>>>>>>>>>>>>>>>>>>")


        spark.close()
    }

    //redis返回的是Java类，为了用map操作引入JavaConversions
    import scala.collection.JavaConversions._
    //从Redis读用户最近的K次评分
    def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
        //用户评分数据保存在 uid:[UID] 为key的队列里，value是MID:SCORE
        //keys * -- 查看所有key
        //lrange [key] [start] [end] -- 查看key的从start到end的value
        jedis.lrange("uid:" + uid, 0, num - 1)
                .map {
                    item =>
                        val attr = item.split("\\:") //MID:SCORE  \\是否多余?
                        (attr(0).trim.toInt, attr(1).trim.toDouble)
                }.toArray

    }

    /**
     * 从相似度矩阵中,获取和当前电影最相似的num个电影，作为推荐备选列表
     * @param num       相似电影数量
     * @param mid       当前电影ID
     * @param uid       当前评分用户ID
     * @param simMovies 相似度矩阵（广播变量）
     * @return          过滤掉已评过分的备选电影列表
     */
    def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                       (implicit mongoConfig: MongoConfig): Array[Int] = {
        //step1: 从相似度矩阵中拿到所有相似的电影
        //simMovies里是[mid, ((mid, score), (mid, score), ...)]
        val allSimMovies = simMovies(mid).toArray

        //step2: 从MongoDB中查询用户已看过的电影
        val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
                .find(MongoDBObject("uid" -> uid)) //相当于db.collection.find("uid" = uid)
                .toArray
                .map {
                    item => item.get("mid").toString.toInt //只需要看过电影的mid
                }

        //step3: 把看过的电影过滤，得到最终推荐列表
        allSimMovies.filter(x => !ratingExist.contains(x._1))
                .sortWith(_._2 > _._2)
                .take(num)
                .map(_._1)  //只需要mid
    }
}
