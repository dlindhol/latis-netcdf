package latis.reader.tsml

import java.io.{File, FileNotFoundException}

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

  //TODO: handle first, last, and limit filter and selections?
  
  private var ncFile: NetcdfFile = null
  
  private def getNcVar(name: String): ucar.nc2.Variable = {
    //Some names contain "." which findVariable will interpret as a structure member
    //NetCDF library dropped NetcdfFile.escapeName between 4.2 and 4.3 so replicate with what it used to do.
    //TODO: replace with "_"?
    val escapedName = EscapeStrings.backslashEscape(name, ".")
    //val vname = vname.replaceAll("""\.""", """\\.""")
    ncFile.findVariable(escapedName)
  }
  
  private def getScaleFactor(ncvar: ucar.nc2.Variable): Double = {
    ncvar.findAttribute("scale_factor") match {
      case att: ucar.nc2.Attribute => att.getNumericValue.doubleValue
      case null => 1.0
    }
  }

  private def readData(ncvar: ucar.nc2.Variable, section: String = ""): ucar.ma2.Array = {
    if (section == "") ncvar.read
    else ncvar.read(section).reduce //drop extra dimensions
  }

  // Note: this method is public mostly so that a subclass of NetcdfAdapter in
  // Lisird3 can use it.
  def readData(ncvarName: String): ucar.ma2.Array = {
    val ncvar = getNcVar(ncvarName)
    if(ncvar == null) {
      throw new RuntimeException("Failed to find ncvar '" + ncvarName + "'")
    }
    else {
      return readData(ncvar)
    }
  }

  override def init {
    //open the NetCDF file as defined by the location in the tsml.
    val location = getUrl.toString
    try {
      ncFile = NetcdfDataset.openFile(location, null)
    }
    catch {
      case e: FileNotFoundException => {
        // hacky workaround for LISIRDIII-719
        val basename = location.substring(0, location.lastIndexOf(File.pathSeparator))
        val baseDir = new File(basename)
        baseDir.listFiles()

        // try to read file one more time. If it throws this time, just let it.
        ncFile = NetcdfDataset.openFile(location, null)
      }
    }

    //TODO: capture attributes in metadata

    //For each variable defined in the tsml, read all the data from the 
    //NetCDF file and put it in the cache.
    for (v <- getOrigScalars) {
      //use origName if it is defined
      val vname = v.getMetadata("origName") match {
        case Some(s) => s
        case None => v.getName
      }

      val ncvar = getNcVar(vname);
      if(ncvar == null) {
        // a null ncvar will throw an exception in just a sec, so we may as
        // well replace it with a more helpful error message.
        throw new RuntimeException("Failed to find ncvar '" + vname + "'");
      }

      val section = v.getMetadata("section") match {
        case Some(s) => s
        case None => ""
      }
      
      //support scale_factor for reals
      val scale = getScaleFactor(ncvar)
      //TODO: support offset

      //Get data Array from Variable.
      //Apply optional 'section' property.
      val ncarray = readData(ncvar, section)

      val n = ncarray.getSize.toInt //TODO: limiting length to int
      //val ds = (0 until n).map(ncarray.getObject(_)).map(Data(_)) //Let Data figure out how to store it, assuming primitive type

      //Store data based on the type of varible as defined in the tsml
      val datas = v match {
        case i: Integer => (0 until n).map(ncarray.getLong(_)).map(Data(_))
        case r: Real => (0 until n).map(ncarray.getDouble(_) * scale).map(Data(_))
        case t: Text => (0 until n).map(ncarray.getObject(_)).map(o => Data(o.toString))
      }

      val data = DataSeq(datas)

      cache(v.getName, data)
    }
  }

  def close = ncFile.close
}

