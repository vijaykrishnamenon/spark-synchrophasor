package edu.amrita.cb.cen.synchrophasor.core

import java.net.{ ServerSocket, Socket }
import java.io.{ BufferedInputStream, OutputStreamWriter, PrintStream }
import crc.CRC_CCITT


object Frame {

  private val crc16 = new CRC_CCITT
  
  private val config2FrameMap = IndexedSeq (
    ("SYNC",2), 
    ("FRAMESIZE",2), 
    ("IDCODE",2), 
    ("SOC",4),
    ("FRACSEC",4), 
    ("TIME_BASE",4),
    ("NUM_PMU",2), 
    ("STN",16),
    ("IDCODE",2),
    ("FORMAT",2), 
    ("PHNMR",2), 
    ("ANNMR",2),
    ("DGNMR",2), 
    ("CHNAM",0), 
    ("PHUNIT",0),
    ("ANUNIT",0), 
    ("DIGUNIT",0), 
    ("FNOM",2), 
    ("CFGCNT",2), 
    ("DATA_RATE",2)
  )

  private val dataFrameMap = IndexedSeq(
    ("SYNC", 2),
    ("FRAMESIZE", 2),
    ("IDCODE", 2),
    ("SOC", 4),
    ("FRACSEC", 4),
    ("STAT", 2),
    ("PHASORS", 0), 
    ("FREQ", 0), 
    ("DFREQ", 0), 
    ("ANALOG", 0), 
    ("DIGITAL", 0)  
  )
  
  /*
   * The toInt / toLong / toString functions make sure the Byte 
   * to Integer / Long / String conversion is smooth and without 
   * negations.
   */
  @inline def toInt(b: Byte) = b & 0x000000FF
  @inline def toInt(ub:Byte, lb:Byte): Int = (0x00000000 | toInt(ub) << 8) | toInt(lb)
  @inline def toInt(bs:IndexedSeq[Byte]): Int = (0 to 3).map( j => ( 0x00000000 | toInt( bs(j) ) << (3-j)*8) ).reduce(_|_)
  @inline def toLong(bs:IndexedSeq[Byte]): Long = (0 to 3).map( j => ( 0x0000000000000000 | toInt( bs(j) ) << (3-j)*8) ).reduce(_|_)
  @inline def toString(bs:IndexedSeq[Byte]) =  new String(bs.toArray).trim 
  @inline def toFloat(bs:IndexedSeq[Byte]) = java.lang.Float.intBitsToFloat(toInt(bs))
  
  def readFrame(in: BufferedInputStream) = {
    
    val buff = Array.fill[Byte](70000)(0x00.toByte)
    // Reading 'SYNC' and 'FRAMESIZE' Bytes...
    in.read(buff, 0, 4)
    
    if (buff(0) == 0xAA.toByte) {

      val sync: Int = toInt(buff(0), buff(1))

      val cat = sync match {
        case 0xAA27 => "Command"
        case 0xAA21 => "Config-1"
        case 0xAA31 => "Config-2"
        case 0xAA52 => "Config-3"
        case 0xAA11 => "Header"
        case 0xAA01 => "Data"
        case _ => "Error"
      }

      //println("Frame type:" + cat)*/

      val fsize: Int = toInt( buff(2), buff(3) )
      
      //println("Frame size:" + fsize)
         
      // Reading the remaining frame ...
      in.mark(fsize)
      var flag = in.read(buff, 4, fsize - 4)

      while(flag != (fsize - 4)) {
        in.reset
        Thread.sleep(1)
        flag = in.read(buff, 4, fsize - 4)
      }      
      
      //Print frame bytes....
      //(0 until fsize).foreach(i => print(toInt(buff(i)).toHexString + " "))

      //Isolate the frame for processing
      val frame = buff.slice(0, fsize - 2) 
     
      // Checking CRC
      val readCRC = toInt( buff(fsize - 2), buff(fsize - 1) ) 
      val computedCRC = crc16.crc_ccitt(frame) 
           
      //Print CRCs ...
      //println("\n"+readCRC+" "+computedCRC)

      if (readCRC == computedCRC)   frame.toIndexedSeq 
      else throw new Exception("CRC Error: Frame Discarded")   
    } 
    else throw new Exception("Illegal Byte read: was expecting a 'SYNC' Byte")
  }

  /*
   * Parse Config-2 Frame ....
   */
  def parseConfig2 (frame: IndexedSeq[Byte]) = { 
    // Index for the configMap ...
    var i = 1  
    // Index for the bit frame array ..
    var b = 2
    
    var fv = List[(String,Any)]()
    val nmr = Array(0,0,0)
    var pmunum = 1
    var pmucount = 1
    val format = Array(true,true,true,false)
    var f = ("",0)
    
    // For ever entry in the frame map ...
    while (i < config2FrameMap.length) {
      f  = config2FrameMap(i)
      val v = f._2 match {
      
      case 2 => {                   
          i match {
            // FORMAT field bit flags..
            case 9 => {              
              val frmt = toInt(frame(b+1))
              
              if((frmt & 0x00000008) != 8) format(0) = false // 0 = FREQ/DFREQ 16-bit integer, 1 = floating point
              if((frmt & 0x00000004) != 4) format(1) = false // 0 = analogs 16-bit integer, 1 = floating point
              if((frmt & 0x00000002) != 2) format(2) = false // 0 = phasors 16-bit integer, 1 = floating point
              if((frmt & 0x00000001) == 1) format(3) = true  // 0 = phasor real and imaginary (rectangular), 1 = magnitude and angle (polar)
              
              format.toList
            }
            // FNOM nominal frequency of the bus being observed ...
            case 17 =>{
              if (toInt(frame(b+1)) == 1) "50 Hz." 
              else "60 Hz."  
            }
            // rest of the cases of 2-Byte fields
            case _ => { 
              val n = toInt(frame(b), frame(b + 1))
              // Repeat STN(8) through CFGCNT(19) 'number of PMU' times
              if(i == 6) pmunum = n
              // saving PHNMR, ANNMR, DGNMR to be used for further calculations like conversion factors. 
              if(i > 9 && i < 13) nmr(i - 10) = n
              n
            }          
          }            
        }
        case 4 => { 
          val j = toLong(frame.slice(b, b+4))
          if(i == 4) (toInt(frame(b+3)), j >> 8) else j
        }
        case 16 => toString(frame.slice(b, b+16))
        case 0 => i match {
        // 'CHNAM' field,  channel names in ASCII text.   
        case 13 => { 
            // Phasor channel names...
            val chp = (1 to nmr(0)).map { i => 
              toString(frame.slice(b + (i-1) * 16, b + i * 16) ) }
            // Analog channel names...
            val cha = (1 to nmr(1)).map { i => 
              toString(frame.slice(b + (i-1) * 16, b + i * 16)) }
            
            // Yet to handle digital flag channel names.          
           
            b += 16*(nmr(0)+nmr(1)+16*nmr(2))
            (chp++cha)
          }
          // Conversion factor for phasor channels; four bytes per phasor.
          case 14 => if(nmr(0) != 0 ) 
            (1 to  nmr(0)).map { i => 
              val k = toInt(frame.slice(b, b+4)); b += 4
              // The Most Significant Bit Indicates it as a current (I) or Voltage (V) phasor
              // if format is floating point the scaling value is ignored or set as 0 ...
              (if(k >> 24 == 0) 'V' else 'I', if(!format(2)) k & 0x00FFFFFF else 0)  
            }
            else ""
          // Conversion factor for analog channels; four bytes per analog value.
          case 15 => if(nmr(1) != 0) 
            (1 to nmr(1)).map { i => 
              val k = toInt(frame.slice(b, b + 4)); b += 4
              // The Most significant Byte is the measurement method Point On Wave (PoW)..
              // ..Root Mean Square(RMS) or Peak value ...
              // Last 24-bit value is user define scaling factor ...
              ( k >> 24 match {
                case 0 => "PoW"
                case 1 => "RMS"
                case 2 => "Peak" 
              }, k & 0x00FFFFFF)  
             }
            else ""
          // 
          case 16 => if(nmr(2) != 0) 
            (1 to nmr(2)).map { i => toInt(frame.slice(b, b + 4)); b += 4 }
            else ""
        }
      }  
      val f_aug = if(i > 6 && i < 19) pmucount + "-" else ""
      fv = ( f_aug + f._1, v ) :: fv   
      b += f._2
      
      if(pmucount < pmunum && i == 18) { 
        pmucount += 1
        i = 7
      }else  i += 1
    }
    fv
  }
  
  def parseDataDetail(frame: IndexedSeq[Byte], 
                numPMUs: Int = 1, 
                phnmr: Int = 3, 
                annmr: Int = 0, 
                dgnmr: Int = 0,
                format: IndexedSeq[Boolean] = IndexedSeq(true,true,true,false),
                factor: IndexedSeq[Int] = IndexedSeq(0,0,0)
                ) = {
    // Indices for format and factor arguments
    val freq = 0
    val phasor = 2
    val analog = 1
    val coordinate = 3
    
    // Index for the dataMap ...
    var i = 1  
    // Index for the bit frame array ...
    var b = 2
    //The list to hold key value pairs ...
    var fv = List[(String,Any)]()
    var f = ("",0)
    var pmucount = 1
       
    while (i < dataFrameMap.length ) {
      
      f  = dataFrameMap(i)
      //println(i + " " +b + " "+ f)
      
      val v = f._2 match {      
      
        case 2 => i match { 
          case 5 => val stat = toInt(frame(b), frame(b+1))
            (toInt(frame(b)) >> 6 match {
              case 0 => "Good Measurement"
              case 1 => "PMU error. No information about data"
              case 2 => "PMU in test mode (do not use values) or"
              case 3 => "PMU error (do not use values)"  
            },
            if((stat & 0x00002000) == 0) "in sync with UTC" else "Not in sync with UTC",
            "Data Sorting " + (if((stat & 0x00001000) == 0) "by timestamp" else "by arival"),
            if((stat & 0x00000800) == 0) "Trigger detected" else "No trigger",
            if((stat & 0x00000400) == 0) "Config change effected" else "1 min. to config change",
            if((stat & 0x00000200) == 0) "Data Modified" else "Data not modified"
            // More flags to handle ...
            )
          case _ =>toInt(frame(b), frame(b+1))
        }
        case 4 => { 
          val j = toLong(frame.slice(b, b+4))
          if(i == 4) (toInt(frame(b+3)), j >> 8) else j
        }        
        case 0 => i match {
          //Extracting phasor values ...
          case 6 => if(format(phasor)) 
            (1 to phnmr).map{ i => 
                val rm = toFloat(frame.slice(b, b + 4))
                val ia = toFloat(frame.slice(b + 4, b + 8))
                b += 8; (rm,ia)
            }
            // Integer format; value not scaled to 'factor' ...
            else {  
              (1 to phnmr).map{ i => 
                b += 4
                (toInt(frame(b-4),frame(b-3)), toInt(frame(b-2),frame(b-1)) ) 
              }
            }
          //Extracting FREQ 
          case 7 => if(format(freq)) { 
              b += 4              
              toFloat(frame.slice(b-4, b))
            }
            // Integer format for FREQ; value not scaled to factor ...
            else { 
              b += 2; toInt( frame(b-2),frame(b-1) ) 
            }
          //Extracting ROCOF alias DFREQ... 
          case 8 => if(format(freq)) { 
              b += 4
              toFloat(frame.slice(b-4, b))
            }
            // Integer format for DFREQ; value not scaled to factor ...
            else { b += 2; toInt(frame(b-2),frame(b-1) ) }
          //Extracting analog values ...
          case 9 => if(format(analog)) 
            (1 to annmr).map{ i => 
                b += 4;
                toFloat(frame.slice(b-4, b))
            }
            // Integer format; value not scaled to 'factor' ...
            else { 
              (1 to annmr).map{ i => 
                b += 2
                toInt(frame(b-2),frame(b-1)) 
              }
            }
          // Extracting DIGITAL flags...
          case 10 => (1 to dgnmr).map{ i => 
                b += 2
                toInt(frame(b-2),frame(b-1)) 
              }
        }  
      }  
      val f_aug = if(i > 4 && i < 11) pmucount + "-" else ""
      fv = ( f_aug + f._1, v ) :: fv   
      b += f._2
      
      if(pmucount < numPMUs && i == 10) { 
        pmucount += 1
        i = 5
      }else  i += 1
    }
    fv
  }
  
  // Creates a command frame with the specified command ...
  def cmdFrame(cmd: Short) = {

    val time = System.currentTimeMillis

    val sync: Short = 0xAA41.toShort
    val framesize: Short = 18
    val idcode: Short = 1001
    val soc: Int = (time / 1000).toInt
    val fracsec: Int = ((time % 1000).toInt << 8) | 0x00000027 // the last byte for Message time quality as 7 and leap second config as 2

    val arr = Array[Byte](((sync >> 8) & 0xFF).toByte, ((sync) & 0xFF).toByte,
      ((framesize >> 8) & 0xFF).toByte, ((framesize) & 0xFF).toByte,
      ((idcode >> 8) & 0xFF).toByte, ((idcode) & 0xFF).toByte,
      ((soc >> 24) & 0xFF).toByte, ((soc >> 16) & 0xFF).toByte, ((soc >> 8) & 0xFF).toByte, ((soc) & 0xFF).toByte,
      ((fracsec >> 24) & 0xFF).toByte, ((fracsec >> 16) & 0xFF).toByte, ((fracsec >> 8) & 0xFF).toByte, ((fracsec) & 0xFF).toByte,
      ((cmd >> 8) & 0xFF).toByte, ((cmd) & 0xFF).toByte)

    val chk: Int = crc16.crc_ccitt(arr)
    arr ++ Array(((chk >> 8) & 0xFF).toByte, (chk & 0xFF).toByte)

  }

  def main(args: Array[String]) = {

    //val file = new PrintStream("freq.csv")
    val s = new Socket("192.168.10.3",4712) //130.237.53.177", 38100)

    if (s != null) println("Connected")
    else println("not connected")

    val in = new BufferedInputStream(s.getInputStream, 500000)
    val out = s.getOutputStream

    // Stop sending data .... 
    out.write(cmdFrame(0x0001))

       
    // Send config2 request....
    out.write(cmdFrame(0x0005))
   
    var f = readFrame(in)
       var t = System.nanoTime
    var fv = parseConfig2(f)    
        println("\nTime:" + (System.nanoTime - t)/1000000.00 +" ms")
    
    println("-------------------------Config-2---------------------------")
    fv.foreach(p => println(String.format("%-10s| ", p._1) + p._2.toString))
    println("------------------------------------------------------------")
    
    val conf = fv.toMap
    val pmuNum = conf("NUM_PMU").asInstanceOf[Int]
    val phnmr = conf("1-PHNMR").asInstanceOf[Int]
    val annmr = conf("1-ANNMR").asInstanceOf[Int]
    val dgnmr = conf("1-DGNMR").asInstanceOf[Int]
    //val format = conf("1-FORMAT").asInstanceOf[Array[Boolean]]
    
    //Start data transmission ...    
    out.write(cmdFrame(0x0002))
    
    val fcount = 50
      t = System.nanoTime
    val fArr = for (i <- 1 to fcount) yield readFrame(in) 
      println("ReadTime per Frame: "+ (System.nanoTime - t)/(fcount*1000000.00) + " ms.")
    
      // Stop sending data .... 
    out.write(cmdFrame(0x0001))   
    
   // var m: Map[String, Any] = Map()
   // t = System.nanoTime
    
    val tArr = fArr.map{ e => 
      fv = parseDataDetail( e, pmuNum, phnmr, annmr, dgnmr) 
           
       //println("---------------------------Data-----------------------------")
        //fv.foreach(p => println(String.format("%-15s| ", p._1) + p._2.toString))
       //println("------------------------------------------------------------")
    }
    t = System.nanoTime - t
    
    //tArr.foreach(file.println)
    //file.close
    
    // Stop sending data ....         
    println("ParseTime per Frame: " + t/(fcount.toFloat*1000000.0) +" ms.")	
    s.close();

  }
}