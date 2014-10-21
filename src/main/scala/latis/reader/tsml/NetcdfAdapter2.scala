package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import latis.dm._
import latis.util.MappingIterator
import latis.data.SampleData
import latis.data.IterableData
import java.io.RandomAccessFile
import java.nio.ByteOrder
import java.nio.channels.FileChannel.MapMode
import latis.data.buffer.ByteBufferData
import latis.data.SampledData
import latis.data.seq.DataSeq
import latis.util.PeekIterator
import latis.util.DataUtils
import ucar.nc2.NetcdfFile
import ucar.nc2.dataset.NetcdfDataset
import ucar.nc2.util.EscapeStrings
import latis.data.Data
import ucar.ma2.InvalidRangeException

class NetcdfAdapter2(tsml: Tsml) extends SsiAdapter2(tsml){
  
  private var ncFile: NetcdfFile = null
  
  override def init {
    val location = getUrl.toString
    ncFile = NetcdfDataset.openFile(location, null)    
  }

  override def findScalarData(s: Scalar): Option[IterableData] = {
    val name = s.getMetadata("origName") match {
      case Some(s) => s
      case None => s.getName
    }

    val section = s.getMetadata("section") match {
      case Some(s) => s
      case None => ""
    }
    
    val escapedName = EscapeStrings.backslashEscape(name, ".")
    val ncvar = ncFile.findVariable(escapedName)
    
    val it = new NetcdfIterator(ncvar, s)
    
    val recSize = s match {
      case _: Real => 8
      case t: Text => t.getMetadata("length").getOrElse("8").toInt
    }
    
    Some(IterableData(it, recSize))
  }
    
 
  override def close = {ncFile.close}

  class NetcdfIterator(v: ucar.nc2.Variable, lv: Variable) extends PeekIterator[Data] {
    
    val shape = v.getShape
    val sec = lv.getMetadata("section").getOrElse(":,".*(shape.length)).split(",")
    val varsec = sec.reverse.dropWhile(_ != ":").reverse //special logic for handling 0,:,n sections
    val div = varsec.zip(shape).scanRight(1)(_._2*_).drop(1)
    def section = (div.zip(shape).map(z => (counter/z._1)%z._2) ++ sec.drop(varsec.length)).mkString(",")
    var counter = -1
    
    def getNext = if(counter+1 >= div(0)) null else try {
      counter += 1
      val sec = section
      val arr = v.read(sec).reduce
      lv match {
        case _: Text => Data(arr.getObject(0).toString)
        case _: Real => Data(arr.getDouble(0))
      }
    } //catch {case e: InvalidRangeException => null}
  }

}