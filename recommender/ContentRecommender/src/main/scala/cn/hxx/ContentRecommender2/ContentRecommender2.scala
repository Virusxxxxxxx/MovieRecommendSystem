package cn.hxx.ContentRecommender2

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

//电影样例类
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 year: String, language: String, genres: String, actors: String, directors: String)

//定义一个推荐样例类
case class Recommendation(mid: Int, score: Double)

//定义基于电影内容信息(genres)提取出的特征向量的电影相似度列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

//MongoConfig样例类
case class MongoConfig(uri: String, db: String)

object ContentRecommender2 {
    //定义表名和常量
    val MONGODB_MOVIE_COLLECTION = "Movie" //读
    val CONTENT_MOVIE_RECS = "ContentMovieRecs" //写

    def main(args: Array[String]): Unit = {
        //配置
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://mycent:27017/recommender2",
            "mongo.db" -> "recommender2"
        )

        //创建Spark
        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender2")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._
        implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

        //从Movie表中加载数据，并预处理
        val movieTagsDF = spark.read
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_MOVIE_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[Movie]
                .map(
                    // 提取mid, name, genres作为内容特征
                    // 分词器默认按空格做分词，因此这里把genres中原始的按|分割转成按空格分割
                    // [Drama|Horror|Thriller] => [Drama Horror Thriller]
                    // 后面做影评分解，在这里加一项影评
                    x => (x.mid, x.name, x.genres.map(g => if (g == '/') ' ' else g))
                )
                .toDF("mid", "name", "genres") //指定列名
                .cache()

        // 核心部分： 用TF-IDF从内容信息中提取电影特征向量

        // step1: 创建一个分词器，默认按空格分词
        val tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")
        // 用分词器对原始数据做转换，生成新的一列words
        // [Drama Horror Thriller] => [drama, horror, thriller]
        val wordsData = tokenizer.transform(movieTagsDF)

        // step2: 引入HashingTF工具
        // 可以把一个词语序列转换成对应的词频
        // [drama, horror, thriller] => (50,[26,27,36],[1.0,1.0,1.0])
        // 相当于hash散列，50是设定的hash表长度，表示这三个类别散列到在26 27 36的位置上，值为1
        // 其类型是SparseVector(稀疏矩阵), 并且用三元组表示[hash表长, 数据位置, 数据]
        val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)
        val featurizedData = hashingTF.transform(wordsData)

        featurizedData.show(truncate = false)

//        // step3: 引入IDF工具
//        val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
//        // 训练IDF模型
//        val idfModel = idf.fit(featurizedData)
//        // 用模型对rawFeatures进行处理，得到每个词的TF-IDF，作为新的特征向量
//        // => (50,[26,27,36],[2.194720551703029,0.7605551441254693,1.7079767945947977])
//        val rescaledData = idfModel.transform(featurizedData)
//
//        //        rescaledData.show(truncate = false)
//
//        val movieFeatures = rescaledData.map(
//            // 这里目标格式是(mid, features), 即(Int, Array[Double]) (参照OfflineRecommender)
//            // 对SparseVector稀疏矩阵类型使用toArray，可以转换为正常数组 => (0.0,...,1.0,0.0,...,0.0)
//            row => (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray)
//        ).rdd.map(  //转成rdd (mllib里的类型都是rdd, 而ml里的类型是DataFrame)
//            x => (x._1, new DoubleMatrix(x._2))
//        )
//
////        movieFeatures.collect().foreach(println)
//
//        // (后面全是复制Offline里的)对所有电影两两计算相似度
//        // 这里的movieFeatures相当于是把标签hash散列后再TD-IDF的标签权值作为特征
//        // 而Offline里的movieFeatures则是通过隐语义模型得到的隐特征
//        val movieRecs = movieFeatures.cartesian(movieFeatures) //笛卡尔积
//                .filter {
//                    //过滤掉自身, 因为自己和自己做相似度必定是1, 其中_1 = mid
//                    case (movie_a, movie_b) => movie_a._1 != movie_b._1
//                }
//                .map {
//                    case (a, b) => {
//                        //计算电影a和电影b的余弦相似度
//                        val simScore = this.consinSim(a._2, b._2)
//                        (a._1, (b._1, simScore)) //(movieA, (movieB, simScore))
//                    }
//                }
//                //这里filter可以放到SteamingRecommender中的computeMovieScore函数中来过滤simScore
//                .filter(_._2._2 > 0.6) //过滤出相似度评分大于0.6的
//                .groupByKey() //(movieA, ((movieB, score),(movieC, score),...))
//                .map { //封装成MovieRecs
//                    case (mid, items) =>
//                        MovieRecs(
//                            mid,
//                            items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2))
//                        )
//
//                }
//                .toDF()
//        storeRecsInMongoDB(movieRecs, CONTENT_MOVIE_RECS)

        spark.close()
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
