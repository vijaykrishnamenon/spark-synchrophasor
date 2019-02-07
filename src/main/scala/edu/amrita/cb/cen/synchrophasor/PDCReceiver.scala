package edu.amrita.cb.cen.synchrophasor.core

import java.io.{ BufferedInputStream, OutputStream }
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

class PDCReceiver (host: String, port: Int)
  extends Receiver [Map[String, Any]](StorageLevel.MEMORY_ONLY)  {

  var socket: Socket = null
  var in: BufferedInputStream = null
  var out: OutputStream = null
  var configMap: Map[String,Any] = null
    
  // Size of Input Buffer set as 100 MiB ....
  val bSize = 104857600
  
  def onStart() {
    
    try { socket = new Socket(host, port) }
    catch {
      case e: Exception => restart("No Connection", e)
    }
    
    in = new BufferedInputStream(socket.getInputStream, bSize)
    out = socket.getOutputStream
    
     // Stop sending data .... 
    out.write(Frame.cmdFrame(0x0001))
       
    // Send config2 request....
    out.write(Frame.cmdFrame(0x0005))   
    val fv = Frame.parseConfig2(Frame.readFrame(in)).reverse  
            
    // Start sending data .... 
    out.write(Frame.cmdFrame(0x0002))
    
    // Create a configuration map ...
    configMap = fv.toMap
    
    // Print Configuration ...
    println("-------------------------Config-2---------------------------")
    fv.foreach(p => println(String.format("%-10s| ", p._1) + p._2.toString))
    println("------------------------------------------------------------")
    
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run = receive 
    }.start
  }

  def onStop() = {
    
  }

  /** 
   *  Continuously receive the data stream and push to spark storage....  
   */
  private def receive() {
      
    var frameArr: IndexedSeq[IndexedSeq[Byte]] = null
    val framerate = configMap("DATA_RATE").asInstanceOf[Int]
    
    try {     
      // Receive until stopped or connection broken continue reading ...    
      frameArr = for (i <- 0 until framerate/2) yield Frame.readFrame(in) 
      
      // While the stream is alive! ....
      while(!isStopped && frameArr != null) { 
        
        store(frameArr.map( f => Frame.parseDataDetail(f).toMap ).toIterator)
        frameArr = for (i <- 0 until framerate/2) yield Frame.readFrame(in) 
      
      }
      
      // Stop sending data .... 
      out.write(Frame.cmdFrame(0x0001))
     
      socket.close()

      // stop
      
    } catch {
        case e: java.io.IOException =>
          // restart if could not connect to server
          restart("Error getting data", e)
        case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
}

import org.apache.log4j.Logger
import org.apache.log4j.Level

object PDCReceiver {
  
  def main (args: Array[String]) {   

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("PDCReceiver")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
      
    // Use some PDC/ PMU simulator to test this code
    val lines = ssc.receiverStream(new PDCReceiver("127.0.0.1", 4712))
    
    lines.foreachRDD { rdd => //println(rdd.count)
     rdd.map ( fv => fv("1-FREQ") ).foreach(f => print(f +",") )
     //println("")
    }  
    ssc.start
    ssc.awaitTermination
  }
}
