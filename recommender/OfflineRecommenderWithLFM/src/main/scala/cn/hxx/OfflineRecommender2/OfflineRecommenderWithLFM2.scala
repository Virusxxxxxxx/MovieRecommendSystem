package cn.hxx.OfflineRecommender2

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

//由于MLlib里有一个Rating的类，故此处改名
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: String)

//定义一个推荐样例类
case class Recommendation(mid: Int, score: Double)

//定义基于ALS预测评分的用户推荐列表
case class UserRecs(uid: Int, recs: Seq[Recommendation])

//定义基于LFM电影特征向量的电影相似度列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

//MongoConfig样例类
case class MongoConfig(uri: String, db: String)

//基于隐语义模型的离线推荐
object OfflineRecommenderWithLFM2 {
    //定义表名
    val MONGODB_RATING_COLLECTION = "Rating"
    val USER_RECS = "UserRecs"      //用户推荐列表
    val MOVIE_RECS = "MovieRecs"    //电影相似度列表
    //用户最大推荐数
    val USER_MAX_RECOMMENDATION = 20

    def main(args: Array[String]): Unit = {
        //配置
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://mycent:27017/recommender2",
            "mongo.db" -> "recommender2"
        )

        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommenderWithLFM2")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._
        implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))


        //加载数据 得到Rating表
        val ratingRDD = spark.read
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_RATING_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[MovieRating]
                .rdd //转成RDD
                .map(item => (item.uid, item.mid, item.score)) //去掉timestamp
                .cache() //这里用cache,防止后面大量的笛卡尔积造成内存不足

        //从Rating中提取所有的uid和mid，并去重
        /*用户向量和电影向量*/
        val userRDD = ratingRDD.map(_._1).distinct()
        val movieRDD = ratingRDD.map(_._2).distinct()

        //ALS训练隐语义模型
        //ALS第一个参数是Rating类型，所以这里先转换类型
        val trainData = ratingRDD.map(item => Rating(item._1, item._2, item._3))    //Rating(uid, mid, score)
        val (rank, iterations, lambda) = (5, 5, 0.00000001) //ALS参数: 维度，迭代次数，正则项
        val model = ALS.train(trainData, rank, iterations, lambda)  //调库, 使用Rating表中的数据训练模型

        // 得到模型后，即得到基于用户和电影的隐特征，即可预测用户未评分的电影的评分
        // step1: user和movie进行笛卡尔积，得到空的评分矩阵
        val matrix = userRDD.cartesian(movieRDD)
        // step2: 预测评分
        val preRatings = model.predict(matrix)
        // step3: 处理结果,得到预测结果
        //将预测结果降序排序，即相当于得到用户的推荐列表(或许这里应该把已看过的去掉？)
        val userRecs = preRatings
                .filter(_.rating > 0) //过滤评分大于0
                .map(item => (item.user, (item.product, item.rating))) //groupby预处理
                .groupByKey() //(uid, ((mid1,rating1), (mid2,rating2), ...))
                .map {  //用UserRecs封装
                    case (uid, recs)
                    => UserRecs(
                        uid,
                        recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION) //按分数排序，取前20个
                                .map(x => Recommendation(x._1, x._2)) //用样例类封装
                    )
                }
                .toDF()
        // step4: 写入mongo
        storeRecsInMongoDB(userRecs, USER_RECS)

        // 基于电影隐特征，计算相似度矩阵，得到电影的相似度列表
        // step1: 得到电影特征
        val movieFeatures = model.productFeatures.map {
            //把电影的特征放入DoubleMatrix，以便调用jblas计算余弦相似度
            case (mid, features) => (mid, new DoubleMatrix(features))
        }
        //step2: 对所有电影两两计算相似度
        val movieRecs = movieFeatures.cartesian(movieFeatures) //笛卡尔积
                .filter {
                    //过滤掉自身, 因为自己和自己做相似度必定是1, 其中_1 = mid
                    case (movie_a, movie_b) => movie_a._1 != movie_b._1
                }
                .map {
                    case (a, b) => {
                        //计算电影a和电影b的余弦相似度
                        val simScore = this.consinSim(a._2, b._2)
                        (a._1, (b._1, simScore)) //(movieA, (movieB, score))
                    }
                }
                //这里filter可以放到SteamingRecommender中的computeMovieScore函数中来过滤simScore
                .filter(_._2._2 > 0.45) //过滤出相似度评分大于0.6的
                .groupByKey() //(movieA, ((movieB, score),(movieC, score),...))
                .map { //封装成MovieRecs
                    case (mid, items) =>
                        MovieRecs(
                            mid,
                            items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2))
                        )

                }
                .toDF()
        movieRecs.take(10).foreach(println)
        storeRecsInMongoDB(movieRecs, MOVIE_RECS)

        spark.stop()
    }

    //写入MongoDB的函数
    def storeRecsInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
        df.write.option("uri", mongoConfig.uri)
                .option("collection", collection_name)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()
    }

    //余弦相似度函数
    def consinSim(movie_a: DoubleMatrix, movie_b: DoubleMatrix): Double = {
        //dot点积 norm2模长
        movie_a.dot(movie_b) / (movie_a.norm2() * movie_b.norm2())
    }
}
