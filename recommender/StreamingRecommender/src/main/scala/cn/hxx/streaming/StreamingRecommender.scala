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
                        val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)

                        //step2: 从相似度矩阵中取出和当前电影最相似的N个电影，作为推荐备选列表，Array[mid]
                        val candidateMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)

                        //step3: 对每个备选电影，计算推荐优先级，得到实时推荐列表，Array[(mid, score)]
                        val streamRecs = computeMovieScore(candidateMovies, userRecentlyRatings, simMovieMatrixBroadCast.value)

                        //step4: 把推荐数据保存到MongoDB
                        saveDataToMongoDB(uid, streamRecs)
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
     *
     * @param num       相似电影数量
     * @param mid       当前电影ID
     * @param uid       当前评分用户ID
     * @param simMovies 相似度矩阵（广播变量）
     * @return 过滤掉已评过分的备选电影列表
     */
    def getTopSimMovies(num: Int, mid: Int, uid: Int,
                        simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
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
                .map(_._1) //只需要mid
    }

    //根据自定义模型，计算推荐优先级(强度)
    def computeMovieScore(candidateMovies: Array[Int],
                          UserRecentlyRatings: Array[(Int, Double)],
                          simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] = {
        //scores是公式中的E_uq，即每部备选电影的推荐强度
        val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

        //定义增强因子和减弱因子
        val increMap = scala.collection.mutable.HashMap[Int, Int]()
        val decreMap = scala.collection.mutable.HashMap[Int, Int]()

        //分子部分
        for (candidateMovie <- candidateMovies; userRecentlyRating <- UserRecentlyRatings) {
            //备选电影和最近评分电影的相似度, 公式中的sim(q,r)
            val simScore = getMoviesSimScore(candidateMovie, userRecentlyRating._1, simMovies)
            //过滤掉电影相似度列表中备选电影和最近评分电影的相似度不存在的电影
            //if(simScore > 0.6)
            if (simScore != 0.0) {
                //计算备选电影的基础推荐得分
                //这里暂时先把sim(q,r)×R_r放在score里, 后面再做求和取平均操作
                //这里实际得到(movie_q, sim(q,r)×R_r), 每个q有多个r, 后面以q为key做groupby,求sum再除数量即可
                scores += ((candidateMovie, simScore * userRecentlyRating._2))
                //如果用户最近的电影评分 > 3, 则增强因子+1, 反之减弱因子+1
                if(userRecentlyRating._2 > 3)
                    increMap(candidateMovie) = increMap.getOrDefault(candidateMovie, 0) + 1
                else
                    decreMap(candidateMovie) = decreMap.getOrDefault(candidateMovie, 0) + 1
            }
        }

        //根据备选电影的mid做groupby, 根据公式求最终的score
        scores.groupBy(_._1).map{
            //得到Map(mid -> ArrayBuffer[(mid, score)])
            case(mid, scoreList) =>
                (mid, scoreList.map(_._2).sum / scoreList.length
                        + log(increMap.getOrDefault(mid, 1))
                        - log(decreMap.getOrDefault(mid, 1)))
        }.toArray.sortWith(_._2 > _._2)


    }

    //获取备选电影和最近评分电影的相似度函数
    def getMoviesSimScore(mid1: Int, mid2: Int,
                          simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Double = {
        //如果两部电影的相似度在simMovies能找到，则返回，否则返回0.0
        simMovies.get(mid1) match {
            case Some(sim) => sim.get(mid2) match {
                case Some(score) => score
                case None => 0.0
            }
            case None => 0.0
        }
    }

    //自定义log函数，后期可以自行更换底数
    def log(i: Int): Double ={
        val N = 10  //底数, 默认10
        math.log(i) / math.log(N)
    }

    //把实时推荐列表存入mongo
    def saveDataToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit ={
        //定义StreamRecs表的连接
        val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

        //如果uid已存在，则删除
        //这里不能用overwrite来覆写
        // 原因：MongoDB中不是按uid来标识文档，而是自动生成的[_id]，来一个相同的uid，MongoDB不会识别
        streamRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))
        //将StreamRecs存入表中
        streamRecsCollection.insert(MongoDBObject("uid" -> uid,
            "recs" -> streamRecs.map(x => MongoDBObject("mid" -> x._1, "score" -> x._2))))
    }
}
