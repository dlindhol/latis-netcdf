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
import latis.time.Time
import latis.time.TimeFormat
import latis.time.TimeScale
import latis.util.StringUtils
import ucar.ma2.{Range => URange}
import ucar.ma2.Section
import ucar.nc2.NetcdfFile
import ucar.nc2.dataset.NetcdfDataset
import ucar.nc2.util.EscapeStrings

import NetcdfAdapter4._
import latis.ops.filter._
import thredds.catalog.ThreddsMetadata.Variables
import latis.reader.adapter.Adapter
import scala.collection.mutable.Stack

class NetcdfAdapter4(model: Model, properties: Map[String, String]) 
  extends Adapter(model, properties) {

  
  private lazy val ncFile: NetcdfFile = {
    val location = getUrl.toString
    try {
      NetcdfDataset.openFile(location, null)
    } catch {
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

  private lazy val domainVars: Seq[VariableType] = model.getDomainVars

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
    domainVars.foldLeft(zero)(_ += _.getId -> None)
  }

  override def handleOperation(op: Operation): Boolean =
    op match {
      case Selection(vname, o, v) if domainVars.exists(_.hasName(vname)) =>
        val newOp = if (vname == "time" && !StringUtils.isNumeric(v)) {
          val nt = convertTime(vname, v)
          new Selection(vname, o, nt)
        } else {
          op
        }
        operations += newOp
        true
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
      case NearestNeighborFilter(vname, v)
          if domainVars.exists(_.hasName(vname)) =>
        val newOp = if (vname == "time" && !StringUtils.isNumeric(v)) {
          val nt = convertTime(vname, v)
          new NearestNeighborFilter(vname, nt)
        } else {
          op
        }
        operations += newOp
        true
      case _                     =>
        false
    }
    
  private def convertTime(vname: String, value: String): String = {
    // We can get this safely because of the guard.
    val domainVar = domainVars.find(_.hasName(vname)).get
    val units = domainVar.getMetadata("units").getOrElse {
      val msg = "Time variable must have units."
      throw new UnsupportedOperationException(msg)
    }
    val ts = TimeScale(units)
    Time.fromIso(value).convert(ts).getValue.toString
  }
    
  /**
   * Get the NetCDF Variable given a LaTiS Variable name.
   */
  private def getNcVar(vname: String): Option[ucar.nc2.Variable] = {
    val name = {
      val oname = model.findVariableAttribute(vname, "origName")
      oname.getOrElse(vname)
    }

    //Some names contain "." which findVariable will interpret as a structure member
    //NetCDF library dropped NetcdfFile.escapeName between 4.2 and 4.3 so replicate with what it used to do.
    //TODO: replace with "_"?
    val escapedName = EscapeStrings.backslashEscape(name, ".")
    val ncvar = ncFile.findVariable(escapedName)

    if (ncvar == null) None
    else Option(ncvar)
  }

  // Given LaTiS Variable primary name.
  private def readData(vname: String): Option[ucar.ma2.Array] = {
    for {
      ncvar <- getNcVar(vname)
      section <- makeSection(vname)
    } yield ncvar.read(section).reduce
  }

  /**
   * Make a Section for a range variable.
   * This assumes that the variable is a function of all domain variables.
   */
  private def makeSection(vname: String): Option[Section] = {
    val dims = model.findVariableAttribute(vname, "shape") match {
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
  private def buildIndex(vname: String): Unit = getNcVar(vname) match {
    case Some(ncvar) if (!indexMap.isDefinedAt(vname)) =>
      val read: (ucar.ma2.Array, Int) => Double =
        domainVars.find(_.hasName(vname)) match {
          case Some(t) if t.getType == "text" =>
            val formatStr = t.getMetadata("units").get
            val tf = TimeFormat(formatStr)
            (arr, i) => {
              val x = arr.getObject(i).toString
              tf.parse(x).toDouble
            }
          case _ =>
            (arr, i) => arr.getDouble(i)
        }

      val index: Array[Double] = {
        val ncarray = ncvar.read

        val n = ncarray.getSize.toInt
        val arr: Array[Double] = new Array(n)
        for (i <- 0 until n) {
          arr(i) = read(ncarray, i)
        }
        arr
      }
      indexMap += vname -> index
  }

  /**
   * Read the data for a given LaTiS Variable by its primary name.
   */
  protected def readIntoCache(vname: String): Unit = {
    //TODO: apply scale and offset
    readData(vname) match {
      case None          => cache(vname, DataSeq())
      case Some(ncarray) =>
        val n = ncarray.getSize.toInt
        //Store data based on the type of variable
        val datas = model.findVariableAttribute(vname, "type") match {
          case Some("integer") =>
            (0 until n).map(ncarray.getLong(_)).map(Data(_))
          case Some("real") =>
            (0 until n).map(ncarray.getDouble(_)).map(Data(_))
          case Some("text") =>
            (0 until n).map(ncarray.getObject(_)).map(o => Data(o.toString))
          case Some(s) => throw new RuntimeException(s"Bad type for $vname: $s")
          case None    => throw new RuntimeException(s"No type defined for $vname")
        }
        cache(vname, DataSeq(datas))
    }
  }

  def replaceLengthMetadata(m: Model): Model = {
    // Collect the length of each Function
    val rs = Stack() ++ ranges.toSeq.collect {
      case (_, Some(r)) => r.length
      case (k, None)    => getNcVar(k) match {
        case Some(ncvar) => ncvar.getSize.toInt
        // If the variable is not in the file, get from the tsml
        case None => m.findVariableByName(k) match {
          case Some(v) => v.getMetadata("length") match {
            case Some(s) => s.toInt //TODO: parse error
            case None => ??? //TODO: error: length not defined for derived variable
          }
          case None => ??? //TODO: error: variable not in tsml, shouldn't happen
        }
      }
    }
    
    m.map {
      case FunctionType(c,d,id,md) => 
        FunctionType(c,d,id, md + ("length", s"${rs.pop}"))
      case vt => vt
    }
  }
  

  /**
   * Get the length of the first domain dimension based on the current
   * state of the ranges.
   */
  def lengthOfFirstDomainDimension: Int = ranges.head match {
    // Assumes 1D but will work with extra dims of size 1.
    case (k, None) => getNcVar(k) match {
      case Some(ncvar) => ncvar.getSize.toInt
      case None => ??? //first dim can't be derived in tsml for now
    }
    case (_, Some(range)) => range.length
  }

  /**
   * Apply each operation to the Range for each domain variable.
   */
  private def applyOperations: Unit = {
    operations.foreach {
      case Selection(vname, op, value) =>
        indexMap.get(vname).foreach { index =>
          queryIndex(index, op, value.toDouble) match {
            case None        => ranges += vname -> Option(new URange(0))
            case Some(range) =>
              /*
               * This lookup should never fail (the only Selections
               * allowed are ones with names that come from the set of
               * domain variables used for these keys) but we can't
               * statically prove this.
               */
              ranges.get(vname).foreach {
                case None    => ranges += vname -> Option(range)
                case Some(r) => ranges += vname -> Option(r intersect range)
              }
          }
        }
      case NearestNeighborFilter(vname, value) =>
        indexMap.get(vname).foreach { index =>
          queryIndex(index, "~", value.toDouble) match {
            case None        => ranges += vname -> Option(new URange(0))
            case Some(range) =>
              /*
               * This lookup should never fail (the only
               * NearestNeighborFilters allowed are ones with names
               * that come from the set of domain variables used for
               * these keys) but we can't statically prove this.
               */
              ranges.get(vname).foreach {
                case None    => ranges += vname -> Option(range)
                case Some(r) => ranges += vname -> Option(r intersect range)
              }
          }
        }
      case _: FirstFilter =>
        val range = new URange(1)
        ranges.headOption.foreach {
          case (k, None)    => ranges += k -> Option(range)
          case (k, Some(v)) => ranges += k -> Option(v compose range)
        }
      case _: LastFilter =>
        val range = {
          val len = lengthOfFirstDomainDimension
          new URange(len - 1, len - 1)
        }
        ranges.headOption.foreach {
          case (k, None)    => ranges += k -> Option(range)
          case (k, Some(v)) => ranges += k -> Option(v compose range)
        }
      case LimitFilter(n) =>
        if (n < lengthOfFirstDomainDimension) {
          val range = new URange(n)
          ranges.headOption.foreach {
            case (k, None)    => ranges += k -> Option(range)
            case (k, Some(v)) => ranges += k -> Option(v compose range)
          }
        }
      case op: TakeOperation =>
        if (op.n < lengthOfFirstDomainDimension) {
          val range = new URange(op.n)
          ranges.headOption.foreach {
            case (k, None)    => ranges += k -> Option(range)
            case (k, Some(v)) => ranges += k -> Option(v compose range)
          }
        }
      case op: TakeRightOperation =>
        val start = lengthOfFirstDomainDimension - op.n
        if (start > 0) {
          val range = new URange(start, op.n)
          ranges.headOption.foreach {
            case (k, None)    => ranges += k -> Option(range)
            case (k, Some(v)) => ranges += k -> Option(v compose range)
          }
        }
      case StrideFilter(n) =>
        if (n > 1) {
          val range = {
            val len = lengthOfFirstDomainDimension
            new URange(0, len, n)
          }
          ranges.headOption.foreach {
            case (k, None)    => ranges += k -> Option(range)
            case (k, Some(v)) => ranges += k -> Option(v compose range)
          }
        }
    }
  }

  /**
   * Read all the data into cache using Sections from the Operations.
   */
  override def makeDataset(model: Model): Dataset = {
    // Build indices for domain variables that have some sort of
    // selection on them.
    operations.collect {
      case Selection(vname, _, _)          => vname
      case NearestNeighborFilter(vname, _) => vname
    }.distinct.foreach(buildIndex(_))

    // Apply the Operations that we agreed to handle.
    // Build up map of ranges.
    applyOperations
/*
 * TODO: apply derivation before reads to get time
 * can't apply as Operation until after makeDataset
 * 
 * make a i -> (data, seconds) dataset, derive, then join?
 * back to custom adapter?
 */
    
    // Read the data sections and cache them.
    model.getScalars.foreach(s => readIntoCache(s.getId))

    // Update nested Function length in metadata.
    val m2 = replaceLengthMetadata(model)
    
    // Delegate to super to make the Dataset.
    super.makeDataset(m2)
  }
  
  def close = ncFile.close
}

object NetcdfAdapter4 {
  // Assuming that the data are ordered in ascending order.
  def queryIndex(index: Array[Double], op: String, v: Double): Option[URange] = {
    val len = index.length
    if (len > 0) {
      index.search(v) match {
        case Found(i) => op match {
          case ">"       =>
            if (i+1 < len) {
              Option(new URange(i+1, len-1))
            } else {
              None
            }
          case ">="      => Option(new URange(i, len-1))
          case "=" | "~" => Option(new URange(i, i))
          case "<="      => Option(new URange(0, i))
          case "<"       =>
            if (i-1 >= 0) {
              Option(new URange(0, i-1))
            } else {
              None
            }
        }
        case InsertionPoint(i) => op match {
          case ">" | ">=" =>
            if (i < len) {
              Option(new URange(i, len-1))
            } else {
              None
            }
          case "="        => None
          case "~"        =>
            if (i == 0) {
              // i = 0 implies our query is smaller than the smallest
              // value in the index
              Option(new URange(0, 0))
            } else if (i == len) {
              // i = len implies our query is larger than the largest
              // value in the index
              Option(new URange(len-1, len-1))
            } else {
              // Here we must determine the value in the index nearest
              // to the queried value.

              // We've already handled the i = 0 case, so i-1 should
              // be safe to access.
              val a = index(i-1)
              val b = index(i)
              // a < v < b

              // If v is equidistant from a and b (v - a = b - v), we
              // will round down. This is to be consistent with the
              // NearestNeighborInterpolation strategy.
              if (v - a <= b - v) {
                Option(new URange(i-1, i-1))
              } else {
                Option(new URange(i, i))
              }
            }
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
