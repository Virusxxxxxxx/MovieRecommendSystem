package cn.hxx.ContentRecommender2

import java.util

import cn.hxx.util.AsciiUtil
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

import scala.collection.mutable

//电影样例类
case class Movie(mid: Int, name: String, descri: String, timelong: Double, issue: String,
                 year: Double, language: String, genres: String, actors: String, directors: String)

case class Comment(uid: Int, mid: Int, content: String, timestamp: String)

//定义一个推荐样例类
case class Recommendation(mid: Int, score: Double)

//定义基于电影内容信息(genres)提取出的特征向量的电影相似度列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

//MongoConfig样例类
case class MongoConfig(uri: String, db: String)

// *************************注意：要运行很多小时****************************************
object ContentRecommender2 {
    //定义表名和常量
    val MONGODB_COMMENT_COLLECTION = "Comment" //读
    val MONGODB_MOVIE_COLLECTION = "Movie" //读
    val CONTENT_MOVIE_RECS = "ContentMovieRecs" //写

    //停用词路径
    val STOP_WORD_PATH = "D:\\Scala_workspace\\MovieRecommendSystem\\recommender\\ContentRecommender\\src\\main\\resources\\stopWords"
    //腾讯Word2Vec路径
    val TECENT_WORD2VEC_PATH = "D:\\Scala_workspace\\MovieRecommendSystem\\recommender\\ContentRecommender\\src\\main\\resources\\word2vec_70000-small.txt"

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

        //jieba -> TF-IDF -> word2vec -> cos

        //从Comment表中加载数据
        val commentRow = spark.read
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_COMMENT_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[Comment]
                .map(
                    // 提取uid, mid, content作为内容特征
                    x => (x.uid, x.mid, x.content)
                )

        //载入腾讯word2vec
        val sc = spark.sparkContext
        val row_vec = sc.textFile(TECENT_WORD2VEC_PATH)
        val key_list = row_vec.map {
            line => line.split(" ")(0)
        }.collect()
        val tecentVec = row_vec.map {
            line =>
                val line_cos = line.split(" ")
                val key = line_cos(0)
                val value: Array[Float] = new Array[Float](200)
                for (i <- 1 until line_cos.size) {
                    value(i - 1) = line_cos(i).toFloat
                }
                (key, value)
        }.collectAsMap().toMap
        //设置模型
        val word2vec = new Word2VecModel(tecentVec)
        //广播停用词
        val broadcast_StopWords = sc.broadcast(sc.textFile(STOP_WORD_PATH).collect().toSet)

        //利用jieba + word2vec处理评论
        val commentDF = commentRow.map{
            case (uid, mid, content) =>
                val dbc_comment = AsciiUtil.sbc2dbcCase(content) //全角转半角
                val participle_comment = participleFunc(dbc_comment) //分词 => 我 爱 刘德华
                        .split(" ")
                        .filter(
                            x => !broadcast_StopWords.value.contains(x) // => 刘德华
                                    && key_list.contains(x)) //判断是否在word2vec词典内
                val similar_list = mutable.HashSet[String]() //用HashSet去重
                for (item <- participle_comment) { //遍历刚才得到的分词
                    val like = word2vec.findSynonyms(item, 5) //找相近的五个词
                    for ((item, cos) <- like) {
                        similar_list += item
                    }
                }
                similar_list ++= participle_comment     //保留原始分词
                (uid, mid, similar_list.mkString(" "))
        }.toDF("uid", "mid", "content")

        // 用TF-IDF从内容信息中提取电影特征向量
        // step1: 创建一个分词器，默认按空格分词
        val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
        // 用分词器对原始数据做转换，生成新的一列words
        // [Drama Horror Thriller] => [drama, horror, thriller]
        val wordsData = tokenizer.transform(commentDF)

//        wordsData.show(5, truncate = false)

        // step2: 引入HashingTF工具
        // 可以把一个词语序列转换成对应的词频
        // [drama, horror, thriller] => (50,[26,27,36],[1.0,1.0,1.0])
        // 相当于hash散列，50是设定的hash表长度，表示这三个类别散列到在26 27 36的位置上，值为1
        // 其类型是SparseVector(稀疏矩阵), 并且用三元组表示[hash表长, 数据位置, 数据]
        val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(100)
        val featurizedData = hashingTF.transform(wordsData)

//        featurizedData.show(truncate = false)

        // step3: 引入IDF工具
        val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        // 训练IDF模型
        val idfModel = idf.fit(featurizedData)
        // 用模型对rawFeatures进行处理，得到每个词的TF-IDF，作为新的特征向量
        // => (50,[26,27,36],[2.194720551703029,0.7605551441254693,1.7079767945947977])
        val rescaledData = idfModel.transform(featurizedData)

        rescaledData.show(5,truncate = false)

//        val userFeatures = rescaledData.map(
//            // 这里目标格式是(mid, features), 即(Int, Array[Double]) (参照OfflineRecommender)
//            // 对SparseVector稀疏矩阵类型使用toArray，可以转换为正常数组 => (0.0,...,1.0,0.0,...,0.0)
//            row => (row.getAs[Int]("uid"), row.getAs[SparseVector]("features").toArray)
//        ).rdd.map( //转成rdd (mllib里的类型都是rdd, 而ml里的类型是DataFrame)
//            x => (x._1, new DoubleMatrix(x._2))
//        )
//
//        userFeatures.collect().take(10).foreach(println)

//***************************************************************************************************************

//        //从Movie表中加载数据，并预处理
//        val movieTagsDF = spark.read
//                .option("uri", mongoConfig.uri)
//                .option("collection", MONGODB_MOVIE_COLLECTION)
//                .format("com.mongodb.spark.sql")
//                .load()
//                .limit(5000)
//                .as[Movie]
//                .map(
//                    x => (x.mid, x.name, x.genres.map(g => if (g == '/') ' ' else g), x.descri)
//                )
//                .toDF("mid", "name", "genres", "describe") //指定列名
//
//        val tokenizer1 = new Tokenizer().setInputCol("genres").setOutputCol("words")
//        // 用分词器对原始数据做转换，生成新的一列words
//        // [Drama Horror Thriller] => [drama, horror, thriller]
//        val wordsData1 = tokenizer1.transform(movieTagsDF)
//
//        // step2: 引入HashingTF工具
//        // 可以把一个词语序列转换成对应的词频
//        // [drama, horror, thriller] => (50,[26,27,36],[1.0,1.0,1.0])
//        // 相当于hash散列，50是设定的hash表长度，表示这三个类别散列到在26 27 36的位置上，值为1
//        // 其类型是SparseVector(稀疏矩阵), 并且用三元组表示[hash表长, 数据位置, 数据]
//        val hashingTF1 = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)
//        val featurizedData1 = hashingTF1.transform(wordsData1)
//
//        //        featurizedData.show(truncate = false)
//
//        // step3: 引入IDF工具
//        val idf1 = new IDF().setInputCol("rawFeatures").setOutputCol("features")
//        // 训练IDF模型
//        val idfModel1 = idf1.fit(featurizedData1)
//        // 用模型对rawFeatures进行处理，得到每个词的TF-IDF，作为新的特征向量
//        // => (50,[26,27,36],[2.194720551703029,0.7605551441254693,1.7079767945947977])
//        val rescaledData1 = idfModel1.transform(featurizedData1)
//
//        //        rescaledData.show(truncate = false)
//
//        val movieFeatures = rescaledData1.map(
//            // 这里目标格式是(mid, features), 即(Int, Array[Double]) (参照OfflineRecommender)
//            // 对SparseVector稀疏矩阵类型使用toArray，可以转换为正常数组 => (0.0,...,1.0,0.0,...,0.0)
//            row => (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray)
//        ).rdd.map(  //转成rdd (mllib里的类型都是rdd, 而ml里的类型是DataFrame)
//            x => (x._1, new DoubleMatrix(x._2))
//        )
//
//        // (后面全是复制Offline里的)对所有电影两两计算相似度
//        // 这里的movieFeatures相当于是把标签hash散列后再TD-IDF的标签权值作为特征
//        // 而Offline里的movieFeatures则是通过隐语义模型得到的隐特征
//        val movieRecs = userFeatures.cartesian(movieFeatures) //笛卡尔积
//                .map {
//                    case (a, b) => {
//                        //计算用户和电影的余弦相似度
//                        val simScore = this.consinSim(a._2, b._2)
//                        (a._1, (b._1, simScore)) //(user, (movie, simScore))
//                    }
//                }
//                //这里filter可以放到SteamingRecommender中的computeMovieScore函数中来过滤simScore
//                .filter(_._2._2 > 0.6) //过滤出相似度评分大于0.6的
//                .groupByKey() //(user, ((movieB, score),(movieC, score),...)
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

    //jieba分词函数
    def participleFunc(content: String): String = {
        val segmenter = new JiebaSegmenter()
        val word_list = segmenter.process(content.trim, JiebaSegmenter.SegMode.INDEX)
                .toArray()
                .map(_.asInstanceOf[SegToken].word)
        val ls = new util.ArrayList[String]()
        word_list.filter(_.length > 1).mkString(" ")
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
