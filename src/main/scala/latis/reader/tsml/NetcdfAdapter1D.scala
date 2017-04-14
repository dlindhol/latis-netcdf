package latis.reader.tsml

import java.io.{File, FileNotFoundException}

import scala.collection.mutable

import latis.dm._
import latis.data.Data
import latis.data.seq.DataSeq
import latis.ops.Operation
import latis.ops.filter.FirstFilter
import latis.ops.filter.LastFilter
import latis.ops.filter.LimitFilter
import latis.ops.filter.StrideFilter
import latis.ops.filter.TakeOperation
import latis.ops.filter.TakeRightOperation
import latis.reader.tsml.ml.Tsml
import ucar.ma2.Section
import ucar.nc2.NetcdfFile
import ucar.nc2.dataset.NetcdfDataset
import ucar.nc2.util.EscapeStrings

/**
 * Simple NetCDF file adapter that reads all the variables defined in
 * the TSML and puts the data into the cache. There are optimizations
 * for certain operations that make the assumption that the underlying
 * NetCDF variables are one-dimensional.
 */
class NetcdfAdapter1D(tsml: Tsml) extends TsmlAdapter(tsml) {

  private var ncFile: NetcdfFile = null

  private val operations = mutable.ArrayBuffer[Operation]()

  override def handleOperation(op: Operation): Boolean = op match {
    case _: FirstFilter        =>
      operations += op
      true
    case _: LastFilter         =>
      operations += op
      true
    case _: LimitFilter        =>
      operations += op
      true
    case _: TakeOperation      =>
      operations += op
      true
    case _: TakeRightOperation =>
      operations += op
      true
    case _: StrideFilter       =>
      operations += op
      true
    case _                     => false
  }

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

  // Note: this method is public mostly so that a subclass of NetcdfAdapter in
  // Lisird3 can use it.
  def readData(ncvarName: String): ucar.ma2.Array = {
    val ncvar = getNcVar(ncvarName)
    if(ncvar == null) {
      throw new RuntimeException("Failed to find ncvar '" + ncvarName + "'")
    }
    else {
      return ncvar.read()
    }
  }

  override def init = {
    //open the NetCDF file as defined by the location in the tsml.
    val location = getUrl.toString
    try {
      ncFile = NetcdfDataset.openFile(location, null)
    }
    catch {
      case e: FileNotFoundException => {
        // hacky workaround for LISIRDIII-719
        val path = getUrl.getPath
        val basename = path.substring(0, path.lastIndexOf(File.separator))
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
      val name = v.getName

      //use origName if it is defined
      val vname = tsml.findVariableAttribute(name, "origName").getOrElse(name)

      val ncvar = getNcVar(vname);
      if(ncvar == null) {
        // a null ncvar will throw an exception in just a sec, so we may as
        // well replace it with a more helpful error message.
        throw new RuntimeException("Failed to find ncvar '" + vname + "'");
      }

      val section = {
        val len = ncvar.getShape(0)
        val orig = tsml.findVariableAttribute(name, "section") match {
          case Some(s) => new Section(s)
          case None    => new Section(Array(len))
        }

        operations.foldLeft(orig) { (s, op) =>
          val s2 = op match {
            case _: FirstFilter         =>
              new Section(Array(1))
            case _: LastFilter          =>
              new Section(Array(len-1), Array(1))
            case LimitFilter(n)         =>
              new Section(Array(n))
            case op: TakeOperation      =>
              new Section(Array(op.n))
            case op: TakeRightOperation =>
              new Section(Array(len - op.n), Array(op.n))
            case StrideFilter(n)        =>
              new Section(Array(0), Array(len), Array(n))
          }
          s compose s2
        }
      }

      //support scale_factor for reals
      val scale = getScaleFactor(ncvar)
      //TODO: support offset

      //Get data Array from Variable.
      val ncarray = ncvar.read(section).reduce

      val n = ncarray.getSize.toInt //TODO: limiting length to int

      //Store data based on the type of varible as defined in the tsml
      val datas = v match {
        case i: Integer => (0 until n).map(ncarray.getLong(_)).map(Data(_))
        case r: Real => (0 until n).map(ncarray.getDouble(_) * scale).map(Data(_))
        case t: Text => (0 until n).map(ncarray.getObject(_)).map(o => Data(o.toString))
      }

      val data = DataSeq(datas)

      cache(name, data)
    }
  }

  def close = ncFile.close
}

