import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//电影样例类
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

//定义一个推荐样例类
case class Recommendation(mid: Int, score: Double)

//定义基于电影内容信息(genres)提取出的特征向量的电影相似度列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

//MongoConfig样例类
case class MongoConfig(uri: String, db: String)

object ContentRecommender {
    //定义表名和常量
    val MONGODB_MOVIE_COLLECTION = "Movie" //读
    val CONTENT_MOVIE_RECS = "ContentMovieRecs" //写

    def main(args: Array[String]): Unit = {
        //配置
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://mycent:27017/recommender",
            "mongo.db" -> "recommender"
        )

        //创建Spark
        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")
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
                    // 分词器默认按空格做分词，因此这里把原始的|分割转成空格
                    // 后面做影评分解，在这里加一项影评
                    x => (x.mid, x.name, x.genres.map(g => if (g == '|') ' ' else g))
                )


        spark.close()
    }
}
