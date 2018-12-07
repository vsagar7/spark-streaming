import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Offensive_Word_Count {
  def main(args: Array[String]):Unit = {
    println("This is the task2 of assignment session 26")
	
	val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingExample")
	val sc = new SparkContext(conf)
	
	sc.setLogLevel("WARN")
	println("Spark Context Created")
	
	//create a set of offensive words which we use to compare and filter these words from input string
    val offensive_word_list: Set[String] = Set("idiot", "fool", "bad", "nonsense")
	//print the list of these offensive words
	println(s"$offensive_word_list")
	// Create a local StreamingContext with working thread and batch interval of 20 seconds
	val ssc = new StreamingContext(sc, Seconds(20))
	
	println("Spark Streaming Context Created !")
	// Create a DStream that will connect to hostname:port,localhost:9999
	val lines = ssc.socketTextStream( hostname="localhost", port= 9999)
    // Split each line into words
    val words = lines.flatMap(_.split(" ")).map (x => x)
    // filter the offensive words from input string by using set and count words
	val Offensive_Word_Count = words.filter(x => offensive_word_list.contains(x)).map(x => (x, 1)).reduceByKey(_ + _)
	Offensive_Word_Count.print()
    // Start the computation
	ssc.start()
	// Wait for the computation to terminate
	ssc.awaitTermination()
  } }