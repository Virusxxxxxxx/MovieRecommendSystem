package cn.hxx.StatisticsRecommender

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Movie(mid:Int, name:String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors:String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

//定义一个推荐样例类
case class Recommendation(mid: Int, score: Double)

//定义电影类别Top10推荐样例类
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

//离线统计推荐
object StatisticsRecommender {
    //定义表名 - 读
    val MONGODB_MOVIE_COLLECTION = "cn.hxx.StatisticsRecommender.Movie"
    val MONGODB_RATING_COLLECTION = "cn.hxx.StatisticsRecommender.Rating"

    //定义表名 - 统计结果写入
    val RATE_MORE_MOVIES = "RateMoreMovies" //历史热门电影统计
    val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies" //最近热门电影统计
    val AVERAGE_MOVIES = "AverageMovies" //电影平均得分统计
    val GENRES_TOP_MOVIES = "GenresTopMovies" //每个类别优质电影统计

    def main(args: Array[String]): Unit = {
        //配置
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://mycent:27017/recommender",
            "mongo.db" -> "recommender"
        )

        //sparkConf
        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("cn.hxx.StatisticsRecommender.StatisticsRecommender")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._
        implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

        //从MongoDB加载数据 - read.load()方法
        val ratingDF = spark.read
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_RATING_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[Rating]
                .toDF()
        val movieDF = spark.read
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_MOVIE_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[Movie]
                .toDF()

        //创建一个临时视图，方便进行接下来的统计
        ratingDF.createOrReplaceTempView("ratings")

        //TODO: 不同的统计推荐结果

        // 1. 历史热门电影统计，即历史评分数量最多 => (mid, count)
        val rateMoreMoviesDF = spark.sql("select mid, count(mid) as count from ratings group by mid")
        //查询结束，把结果写回对应的collection
        storeDFInMongoDB(rateMoreMoviesDF, RATE_MORE_MOVIES)


        // 2. 最近热门电影统计，按照月份由近到远，统计每个月的电影评分数量
        // 定义一个日期格式 => yyyyMM
        val simpleDateFormat = new SimpleDateFormat("yyyyMM")
        //注册udf，把时间戳转换成年月格式
        /**
         * 注册udf工具
         * name = changeDate
         * func = 1260759135(秒) * 1000L 由秒变成毫秒, 然后用刚才定义的yyyyMM转换
         * 因为1260759135000超出Int，所以乘以1000L转成Long，最后再转回Int
         */
        spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

        //sql预处理, 去掉uid, 转换timestamp
        val ratingOfYearMonth = spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")
        ratingOfYearMonth.createOrReplaceTempView("ratingOfYearMonth")

        //从临时表中查找，按月份由近到远，每月的每部电影评分次数，降序排序
        val rateMoreRecentlyMoviesDF = spark.sql("select mid, count(mid) as count, yearmonth " +
                "from ratingOfYearMonth group by yearmonth, mid order by yearmonth desc, count desc")
        storeDFInMongoDB(rateMoreRecentlyMoviesDF, RATE_MORE_RECENTLY_MOVIES)


        // 3. 电影平均得分统计
        val averageMoviesDF = spark.sql("select mid, avg(score) as avg from ratings group by mid")
        storeDFInMongoDB(averageMoviesDF, AVERAGE_MOVIES)


        // 4. 每个类别电影Top统计
        // 定义所有类别
        val genres = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery", "Romance", "Science", "Tv", "Thriller", "War", "Western")

        // 平均评分加入加入movie表里(按mid进行inner join)
        val movieWithScore = movieDF.join(averageMoviesDF, "mid")

        // genres转成rdd，方便做笛卡尔积
        val genresRDD = spark.sparkContext.makeRDD(genres)

        //计算类别top10
        val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd) //step1: 笛卡尔积 => (genres, movieInfo)
                .filter { //step2: 条件过滤
                    /**
                     * getAs[String]("genres") => Action|Adventure|Sci-Fi (该条记录的电影的类别)
                     * contains(genre) => 比如这条记录genre是Action，那么该电影包含Action，则保留这行，否则筛掉
                     * 全加上toLowerCase避免大小写敏感
                     */
                    case (genre, movieRow) =>
                        movieRow.getAs[String]("genres").toLowerCase
                                .contains(genre.toLowerCase)
                }
                .map { //step3: 数据结构转换，也可以算是投影 => (genre, (mid, avg))
                    case (genre, movieRow) =>
                        (genre, (movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avg")))

                }
                .groupByKey() //step4: 按genre聚合
                .map { //step5: genre内按avg来sort, 然后take前十
                    /**
                     * 这里用上面定义的样例类来封装数据
                     * cn.hxx.StatisticsRecommender.GenresRecommendation(genre, res) 第二项是Recommendation类型
                     * cn.hxx.StatisticsRecommender.Recommendation(mid, avg)
                     */
                    case (genre, mid_avg) =>
                        GenresRecommendation(
                            genre,
                            mid_avg.toList.sortWith(_._2 > _._2).take(10).map(items => Recommendation(items._1, items._2))
                        )
                }.toDF()
        storeDFInMongoDB(genresTopMoviesDF, GENRES_TOP_MOVIES)

        spark.stop()

    }

    def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
        df.write
                .option("uri", mongoConfig.uri)
                .option("collection", collection_name)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()
    }

}
