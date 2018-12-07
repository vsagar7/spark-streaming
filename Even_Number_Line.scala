import org.apache.spark.{SparkConf,SparkContext}

import org.apache.spark.streaming.{Seconds, StreamingContext}

object Even_Number_Line{

  def main(args: Array[String]):Unit = {

  
    // create a function which
	def Get_Lines_Sum(input : String) : Double ={
	  
	  val line = input.split(" ")
	   var number : Double = 0.0
	  for (x <- line)
	    {
		  try{
		    val value = x.toDouble
			number = number + value
		  }
		  catch
		    {
			  case ex : Exception => {}
			}
		}
	   return number
    }

    println("This is the task1 of assignment session 26")
	
	val conf = new SparkConf().setMaster("local[2]").setAppName("EvenLines")
	val sc = new SparkContext(conf)
	
	sc.setLogLevel("WARN")
	println("Spark Context Created")
	
	// Create a local StreamingContext with working thread and batch interval of 20 seconds
	val ssc = new StreamingContext(sc, Seconds(30))
	
	println("Spark Streaming Context Created")
	
	// Create a DStream that will connect to hostname:port,localhost:9999
	val lines = ssc.socketTextStream( hostname="localhost", port= 9999)
	
	//filter the even string from input line by using Get_Lines function
	val lines_filter = lines.filter(x => Get_Lines_Sum(x)%2 == 0)
	
	//add all the numbers the even string from input line by using Get_Lines function
	val lines_sum = lines_filter.map(x => Get_Lines_Sum(x))

    println("Lines with even sum:")
    lines_filter.print()

    println("Sum of the numbers in even lines:")
    lines_sum.reduce(_+_).print()

    // Start the computation
	ssc.start()
	
	// Wait for the computation to terminate
	ssc.awaitTermination()
  }
  
}