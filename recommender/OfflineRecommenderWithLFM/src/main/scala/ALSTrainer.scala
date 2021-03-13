import OfflineRecommenderWithLFM.MONGODB_RATING_COLLECTION
import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

//通过调整ALS算法的参数，计算均方根误差(RMSE)来评估模型，RMSE越小模型越好
object ALSTrainer {
    def main(args: Array[String]): Unit = {
        //配置
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://mycent:27017/recommender",
            "mongo.db" -> "recommender"
        )

        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ALSTrainer")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._
        implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

        //加载Rating
        val ratingRDD = spark.read
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_RATING_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[MovieRating]
                .rdd //转成RDD
                .map(item => Rating(item.uid, item.mid, item.score)) //去掉timestamp,直接转成Rating类型
                .cache()

        //随机切分数据集，生成训练集和测试集，82开
        val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
        val trainRDD = splits(0)
        val testRDD = splits(1)

        //测试参数函数，输出最优参数
        adjustALSParam(trainRDD, testRDD)

        spark.close()
    }

    //参数调优函数, 使用均方根误差RMSE来衡量模型优劣
    def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {
        val result = for (rank <- Array(50, 100, 200, 300); lambda <- Array(0.01, 0.1, 1))
        //yield的作用：每次循环把结果保存进result，最后result里面是每次循环的结果
            yield {
                val model = ALS.train(trainData, rank, 5, lambda) //训练集训练模型
                val rmse = getRMSE(model, testData) //得到模型，用测试集计算当前参数对应的模型的RMSE
                (rank, lambda, rmse) //把每次循环结果已元组的形式存入result
            }
        println(result.minBy(_._3))
    }

    //计算RMSE函数
    def getRMSE(model: MatrixFactorizationModel, testData: RDD[Rating]): Double = {
        //step1: 把测试集里的uid,mid拿出来
        val userProducts = testData.map(item => (item.user, item.product))
        //step2: 放到model里面进行预测评分
        val predictRating = model.predict(userProducts)

        //接下去拿得到的预测评分和原测试集里已有的评分进行RMSE计算,公式见pdf
        //step3: 把uid, mid提出来准备作为公共属性进行inner join
        val observed = testData.map(item => ((item.user, item.product), item.rating))
        val predict = predictRating.map(item => ((item.user, item.product), item.rating))

        sqrt( //这里参照RMSE公式理解
            observed.join(predict).map {
                //得到((uid, mid), (observed, predict))
                case ((uid, mid), (obs_rating, pre_rating)) =>
                    val delta = obs_rating - pre_rating
                    delta * delta
            }.mean() //求均值函数
        ) //根号
    }
}
