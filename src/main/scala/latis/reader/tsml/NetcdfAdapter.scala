package latis.reader.tsml

import latis.dm._
import latis.data.Data
import latis.data.seq.DataSeq
import latis.reader.tsml.ml.Tsml
import ucar.nc2.NetcdfFile
import ucar.nc2.dataset.NetcdfDataset
import ucar.nc2.util.EscapeStrings

/**
 * Simple NetCDF file adapter that reads all the variables defined in the TSML
 * and puts the data into the cache. Presumably this will work for multidimensional
 * data if the tsml models it correctly.
 */
class NetcdfAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {

  private var ncFile: NetcdfFile = null
  
  override def init {
    //open the NetCDF file as defined by the location in the tsml.
    val location = getUrl.toString
    ncFile = NetcdfDataset.openFile(location, null)
    
    //TODO: capture attributes in metadata
    
    //For each variable defined in the tsml, read all the data from the 
    //NetCDF file and put it in the cache.
    for (v <- getOrigScalars) {
      //use origName if it is defined
      val vname = v.getMetadata("origName") match {
        case Some(s) => s
        case None => v.getName
      }
        
      //Some names contain "." which findVariable will interpret as a structure member
      //NetCDF library dropped NetcdfFile.escapeName between 4.2 and 4.3 so replicate with what it used to do.
      //TODO: replace with "_"?
      val escapedName = EscapeStrings.backslashEscape(vname, ".") 
      //val vname = vname.replaceAll("""\.""", """\\.""")
      val ncvar = ncFile.findVariable(escapedName)
      
      //Get data Array from Variable.
      //Apply optional 'section' property.
      val ncarray = v.getMetadata("section") match {
        case Some(s) => ncvar.read(s).reduce //drop extra dimensions
        case None    => ncvar.read
      }
      
      val n = ncarray.getSize.toInt //TODO: limiting length to int
      //val ds = (0 until n).map(ncarray.getObject(_)).map(Data(_)) //Let Data figure out how to store it, assuming primitive type

      //Store data based on the type of varible as defined in the tsml
      val datas = v match {
        case i: Integer => (0 until n).map(ncarray.getLong(_)).map(Data(_))
        case r: Real    => (0 until n).map(ncarray.getDouble(_)).map(Data(_))
        case t: Text    => (0 until n).map(ncarray.getObject(_)).map(o => Data(o.toString))
      }
      
      val data = DataSeq(datas)
      
      cache(v.getName, data)
    }
  }
  
  def close = ncFile.close
}

