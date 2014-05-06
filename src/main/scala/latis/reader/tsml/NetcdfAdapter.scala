package latis.reader.tsml

import latis.data.Data
import latis.data.seq.DataSeq
import latis.reader.tsml.ml.Tsml
import ucar.nc2.NetcdfFile
import ucar.nc2.dataset.NetcdfDataset

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
    
    //For each variable defined in the tsml, read all the data from the 
    //NetCDF file and put it in the cache.
    for (v <- getOrigScalars) {
      val vname = v.getName
      val ncvar = ncFile.findVariable(vname)
      val ncarray = ncvar.read
      val n = ncarray.getSize.toInt //TODO: limiting length to int
      val ds = (0 until n).map(ncarray.getObject(_)).map(Data(_)) //Let Data figure out how to store it, assuming primitive type
      val data = DataSeq(ds)
      
      cache(vname, data)
    }
  }
  
  def close = ncFile.close
}

