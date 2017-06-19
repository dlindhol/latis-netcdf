package latis.reader.tsml

import java.io.{File, FileNotFoundException}

import scala.annotation.tailrec
import scala.collection.Searching._
import scala.collection.mutable

import latis.dm._
import latis.data.Data
import latis.data.seq.DataSeq
import latis.ops.Operation
import latis.ops.filter.Selection
import latis.reader.tsml.ml.Tsml
import latis.reader.tsml.ml.DatasetMl
import latis.reader.tsml.ml.FunctionMl
import latis.reader.tsml.ml.VariableMl
import latis.time.Time
import latis.time.TimeScale
import latis.util.StringUtils
import ucar.ma2.{Range => URange}
import ucar.ma2.Section
import ucar.nc2.NetcdfFile
import ucar.nc2.dataset.NetcdfDataset
import ucar.nc2.util.EscapeStrings

import NetcdfAdapter3._
import latis.ops.filter._

class NetcdfAdapter3(tsml: Tsml) extends TsmlAdapter(tsml) {
  
  private lazy val ncFile: NetcdfFile = {
    val location = getUrl.toString
    try {
      NetcdfDataset.openFile(location, null)
    }
    catch {
      case e: FileNotFoundException => {
        // hacky workaround for LISIRDIII-719
        val path = getUrl.getPath
        val basename = path.substring(0, path.lastIndexOf(File.separator))
        val baseDir = new File(basename)
        baseDir.listFiles()

        // try to read file one more time. If it throws this time, just let it.
        NetcdfDataset.openFile(location, null)
      }
    }
  }
  
//  private lazy val domainVars: Seq[Scalar] = {
//    // Note, this will break if we get a Tuple domain.
//    def accumulateDomains(v: Variable, acc: Seq[Scalar]): Seq[Scalar] = {
//      v match {
//        case f: Function => f.getDomain match {
//          case s: Scalar => accumulateDomains(f.getRange, acc :+ s)
//          case _ => 
//            val msg = "NetcdfAdapter3 only supports Scalar domain variables."
//            throw new UnsupportedOperationException(msg)
//        }
//        case Tuple(vars) => vars.foldRight(acc)(accumulateDomains(_, _))
//        case _ => acc
//      }
//    }
//      
//    getOrigDataset match {
//      case Dataset(v) => accumulateDomains(v, Seq())
//    }
//  }
    
  private lazy val domainVars: Seq[VariableMl] = {
    @tailrec
    def go(f: FunctionMl, acc: Seq[VariableMl]): Seq[VariableMl] = {
      f.range match {
        case g: FunctionMl => go(g, f.domain +: acc)
        case _             => f.domain +: acc
      }
    }

    // getVariableMl will wrap 'time' and 'index' variables in
    // implicit functions, so we get FunctionMl anyway.
    val mls = tsml.dataset.getVariableMl match {
      case f: FunctionMl => go(f, Seq())
      case _             => Seq()
    }
    mls.reverse
  }
  
  /**
   * A map from the name of a variable to the index for that variable.
   */
  private val indexMap: mutable.Map[String, Array[Double]] =
    mutable.Map()

  private val operations: mutable.ArrayBuffer[Operation] =
    mutable.ArrayBuffer[Operation]()
  
  /**
   * A map from the name of a variable to the range for that variable.
   * Initialize with None to represent a variable that has had no
   * operations applied to its range and opposed to an empty range
   * when an operation eliminates all values for the variable.
   */
  private lazy val ranges: mutable.LinkedHashMap[String, Option[URange]] = {
    val zero = mutable.LinkedHashMap[String, Option[URange]]()
    domainVars.foldLeft(zero)(_ += _.getName -> None)
  }

  
  override def handleOperation(op: Operation): Boolean =
    op match {
      case Selection(vname, o, v)
          if domainVars.exists(_.hasName(vname)) =>
        val newOp = if (vname == "time" && !StringUtils.isNumeric(v)) {
          val units = domainVars.find(_.hasName(vname))
            .map(_.getMetadataAttributes)
            .flatMap(_.get("units")).get
          val ts = TimeScale(units)
          val nt = Time.fromIso(v).convert(ts).getValue
          new Selection(vname, o, nt.toString)
        } else {
          op
        }
        operations += newOp
        true
      
//      if domainVars.exists(_.hasName(vname)) =>
//        val newOp = if (vname == "time" && !StringUtils.isNumeric(v)) {
//          val units = domainVars.find(_.hasName(vname)) match {
//            case Some(v) => v.getMetadata("units") match {
//              case Some(u) => u
//              case None => 
//                val msg = "Time variable must have units."
//                throw new UnsupportedOperationException(msg)
//            }
//            case None => ??? //shouldn't happen
//          }
//          val ts = TimeScale(units)
//          val nt = Time.fromIso(v).convert(ts).getValue
//          new Selection(vname, o, nt.toString)
//        } else {
//          op
//        }
//        operations += newOp
//        true
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
      case _                     =>
        false
    }
    
  // Given LaTiS Variable primary name
  private def getNcVar(vname: String): ucar.nc2.Variable = {
    val name = {
      val oname = tsml.findVariableAttribute(vname, "origName")
      oname.getOrElse(vname)
    }
    
    //Some names contain "." which findVariable will interpret as a structure member
    //NetCDF library dropped NetcdfFile.escapeName between 4.2 and 4.3 so replicate with what it used to do.
    //TODO: replace with "_"?
    val escapedName = EscapeStrings.backslashEscape(name, ".")
    val ncvar = ncFile.findVariable(escapedName)

    if (ncvar == null) {
      throw new RuntimeException("Failed to find ncvar '" + name + "'")
    }

    ncvar
  }

//  private def getScaleFactor(ncvar: ucar.nc2.Variable): Double = {
//    ncvar.findAttribute("scale_factor") match {
//      case att: ucar.nc2.Attribute => att.getNumericValue.doubleValue
//      case null => 1.0
//    }
//  }

  // Given LaTiS Variable primary name.
  private def readData(vname: String): Option[ucar.ma2.Array] = {
    val ncvar = getNcVar(vname)
//    makeSection(vname) map {
//      ncvar.read(_).reduce //drop extra dimensions
//    }
    val section = makeSection(vname).get
    Option(ncvar.read(section).reduce)
  }
  
  /**
   * Make a Section for a range variable.
   * This assumes that the variable is a function of all domain variables.
   */
  private def makeSection(vname: String): Option[Section] = {
    val dims = tsml.findVariableAttribute(vname, "shape") match {
      case Some(s) => s.split(",")
      case None => throw new UnsupportedOperationException("NetCDF variable's 'shape' is not defined.")
    }
    
    val ss = dims.map {
      case s if StringUtils.isNumeric(s) => s
      case s => ranges.get(s) match {
        case Some(or) => or match {
          case Some(r) => 
            if (r.length > 0) r.toString
            else "EMPTY"
          case None    => ":"
        }
        case None => throw new UnsupportedOperationException(s"No range defined for dimension $s.")
      }
    }
    
    if (ss.contains("EMPTY")) None
    else Option(new Section(ss.mkString(",")))
  }

  /**
   * This assumes 1D domains, but it will work if there are more dims of size 1.
   */
  private def buildIndex(vname: String): Unit =
    if (! indexMap.isDefinedAt(vname)) {

      val index: Array[Double] = {
        val ncvar = getNcVar(vname)
        val ncarray = ncvar.read

        val n = ncarray.getSize.toInt
        val arr: Array[Double] = new Array(n)
        for (i <- 0 until n) {
          arr(i) = ncarray.getDouble(i)
        }
        arr
      }
      indexMap += vname -> index
    }

  // Given LaTiS Variable primary name
  private def readIntoCache(vname: String): Unit = {
    //TODO: apply scale and offset
    readData(vname).foreach { ncarray =>
      val n = ncarray.getSize.toInt
      //val datas = (0 until n).map(ncarray.getDouble(_)).map(Data(_))      
      //Store data based on the type of variable
      val datas = getOrigDataset.findVariableByName(vname) match {
        case Some(i: Integer) => (0 until n).map(ncarray.getLong(_)).map(Data(_))
        case Some(r: Real) => (0 until n).map(ncarray.getDouble(_)).map(Data(_))
        case Some(t: Text) => (0 until n).map(ncarray.getObject(_)).map(o => Data(o.toString))
      }
      cache(vname, DataSeq(datas))
    }
  }
  
  override def makeOrigDataset: Dataset = {
    val ds = super.makeOrigDataset
    
    // Build indices for domain variable with Selections.
    operations.collect { case s: Selection => s }
      .map(_.vname).distinct.foreach(buildIndex(_))
    
    // Apply the Operations that we agreed to handle.
    // Build up map of ranges.
    applyOperations
    
    // Update nested Function length in metadata.
    //TODO
    
    ds
  }
  
  /**
   * Get the length of the first domain dimension based on the current
   * state of the ranges.
   */
  def lengthOfFirstDomainDimension: Int = ranges.head match {
    // Assumes 1D but will work with extra dims of size 1.
    case (k, None)        => getNcVar(k).getSize.toInt
    case (_, Some(range)) => range.length
  }
  
  /**
   * Apply each operation to the Range for each domain variable.
   */
  private def applyOperations: Unit = {
    operations.foreach {
//      case s @ Selection(vname, _, _) => {
//        indexMap.get(vname).foreach { index =>
//          val i = domainVars.indexWhere(_.hasName(vname))
//          ranges(i) = if (ranges(i).isEmpty) {
//            queryIndex(index, s)
//          } else {
//            for {
//              r1 <- ranges(i)
//              r2 <- queryIndex(index, s)
//            } yield r1 intersect r2
//          }
//        }
//      }
      case _: FirstFilter => ranges.head match {
        case (k, v) =>
          if (v.isEmpty) ranges += k -> Option(new URange(1))
          else ranges += k -> v.map(_ compose (new URange(1)))
      }
      case _: LastFilter => ranges.head match {
        case (k, v) =>
          val len = lengthOfFirstDomainDimension
          if (v.isEmpty) ranges += k -> Option(new URange(len-1, len-1))
          else ranges += k -> v.map(_ compose (new URange(len-1, len-1)))
      }
//      case _: LastFilter =>
//        ranges(0) = if (ranges(0).isEmpty) {
//          val len = lengthOfFirstDomainDimension
//          Option(new URange(len-1, len-1))
//        } else {
//          ranges(0).map(r => r compose (new URange()))
//        }
//        new Section(Array(len - 1), Array(1))
//      case LimitFilter(n) =>
//        ranges(0) = if (ranges(0).isEmpty) {
//          Option(new URange(1))
//        } else {
//          ranges(0).map(_ compose (new URange(1)))
//        }
//        new Section(Array(n))
//      case op: TakeOperation =>
//        ranges(0) = if (ranges(0).isEmpty) {
//          Option(new URange(1))
//        } else {
//          ranges(0).map(_ compose (new URange(1)))
//        }
//        new Section(Array(op.n))
//      case op: TakeRightOperation =>
//        ranges(0) = if (ranges(0).isEmpty) {
//          Option(new URange(1))
//        } else {
//          ranges(0).map(_ compose (new URange(1)))
//        }
//        new Section(Array(len - op.n), Array(op.n))
//      case StrideFilter(n) =>
//        ranges(0) = if (ranges(0).isEmpty) {
//          Option(new URange(1))
//        } else {
//          ranges(0).map(_ compose (new URange(1)))
//        }
//        new Section(Array(0), Array(len), Array(n))
    }
  }
    
  /**
   * Read all the data into cache using Sections from the Operations.
   */
  override def init = getOrigScalars.foreach(s => readIntoCache(s.getName))

//  // We need to set the "length" of the inner function, which is
//  // defined in the TSML, to the correct value based on the selections
//  // we were given. This requires reading the domain variables from
//  // the NetCDF file and querying our index to figure out how much of
//  // the data we'll actually be reading for the inner function. There
//  // isn't a good mechanism for this at the moment.
//  override def makeOrigDataset: Dataset = {
//    val location = getUrl.toString
//
//    // This is a hack to get around the fact that we need to read the
//    // NetCDF file in order to put accurate metadata in the original
//    // dataset, but the FileJoinAdapter needs to see the original
//    // dataset before it fills out the template with the real location
//    // of the NetCDF file.
//    //
//    // If this adapter is being used in template mode, the path will
//    // contain "..." and otherwise we assume it won't.
//    if (location.contains("...")) {
//      return super.makeOrigDataset
//    }
//
//    try {
//      //open the NetCDF file as defined by the location in the tsml.
//      ncFile = NetcdfDataset.openFile(location, null)
//    }
//    catch {
//      case e: FileNotFoundException => {
//        // hacky workaround for LISIRDIII-719
//        val path = getUrl.getPath
//        val basename = path.substring(0, path.lastIndexOf(File.separator))
//        val baseDir = new File(basename)
//        baseDir.listFiles()
//
//        // try to read file one more time. If it throws this time, just let it.
//        ncFile = NetcdfDataset.openFile(location, null)
//      }
//    }
//
//    // Build the index for domain variables that have selections, only once.
//    operations.collect {
//      case s: Selection => s
//    }.map(_.vname).distinct.foreach(buildIndex(_))
//    
//    
//    
//    // Apply each operation to the Range for each domain variable.
//    operations.foreach {
//      case s @ Selection(vname, _, _) => {
//        indexMap.get(vname).foreach { index =>
//          val i = domainVars.indexWhere(_.hasName(vname))
//          ranges(i) = if (ranges(i).isEmpty) {
//            queryIndex(index, s)
//          } else {
//            for {
//              r1 <- ranges(i)
//              r2 <- queryIndex(index, s)
//            } yield r1 intersect r2
//          }
//        }
//      }
//      case _: FirstFilter =>
//        ranges(0) = if (ranges(0).isEmpty) {
//          Option(new URange(1))
//        } else {
//          ranges(0).map(_ compose (new URange(1)))
//        }
//
//    }
//    
//    //TODO: replicate ranges Map
//    //TODO: aliases in selection might not match vname in file
//    //TODO: consider stride
//
//    val ds = super.makeOrigDataset
//    ds match {
//      case Dataset(f: Function) =>
//        f.getRange match {
//          case g: Function =>
//            val md = ranges(1) match {
//              case Some(r) => g.getMetadata + (("length", s"${r.length}"))
//              case None    => g.getMetadata
//            }
//            Dataset(
//              Function(f.getDomain,
//                Function(g.getDomain, g.getRange, md),
//                f.getMetadata),
//              ds.getMetadata
//            )
//        }
//    }
//  }

  def close = ncFile.close
}

object NetcdfAdapter3 {
  // Assuming that the data are ordered in ascending order.
  def queryIndex(index: Array[Double], s: Selection): Option[URange] = {
    val len = index.length
    if (len > 0) {
      index.search(s.value.toDouble) match {
        case Found(i) => s.operation match {
          case ">"  =>
            if (i+1 < len) {
              Option(new URange(i+1, len-1))
            } else {
              None
            }
          case ">=" => Option(new URange(i, len-1))
          case "==" => Option(new URange(i, i))
          case "<=" => Option(new URange(0, i))
          case "<"  =>
            if (i-1 >= 0) {
              Option(new URange(0, i-1))
            } else {
              None
            }
        }
        case InsertionPoint(i) => s.operation match {
          case ">" | ">=" =>
            if (i < len) {
              Option(new URange(i, len-1))
            } else {
              None
            }
          case "=="       => None
          case "<" | "<=" =>
            if (i > 0) {
              Option(new URange(0, i-1))
            } else {
              None
            }
        }
      }
    } else {
      None
    }
  }

}
