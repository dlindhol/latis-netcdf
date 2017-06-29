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
import latis.reader.tsml.ml.TimeMl
import latis.reader.tsml.ml.VariableMl
import latis.time.Time
import latis.time.TimeFormat
import latis.time.TimeScale
import latis.util.StringUtils
import ucar.ma2.{Range => URange}
import ucar.ma2.Section
import ucar.nc2.NetcdfFile
import ucar.nc2.dataset.NetcdfDataset
import ucar.nc2.util.EscapeStrings

import NetcdfAdapter3._
import latis.ops.filter._
import latis.reader.tsml.ml.TupleMl
import thredds.catalog.ThreddsMetadata.Variables

class NetcdfAdapter3(tsml: Tsml) extends TsmlAdapter(tsml) {

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

  private lazy val domainVars: Seq[VariableMl] =
    getDomainVars(tsml.dataset)

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
      case Selection(vname, o, v) if domainVars.exists(_.hasName(vname)) =>
        val newOp = if (vname == "time" && !StringUtils.isNumeric(v)) {
          // We can get this safely because of the guard.
          val domainVar = domainVars.find(_.hasName(vname)).get
          val units = domainVar.getMetadataAttributes.get("units").getOrElse {
            val msg = "Time variable must have units."
            throw new UnsupportedOperationException(msg)
          }
          val ts = TimeScale(units)
          val nt = Time.fromIso(v).convert(ts).getValue.toString
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

  // Given LaTiS Variable primary name.
  private def readData(vname: String): Option[ucar.ma2.Array] = {
    val ncvar = getNcVar(vname)
    val section = makeSection(vname)
    section.map(ncvar.read(_).reduce)
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
      val read: (ucar.ma2.Array, Int) => Double =
        domainVars.find(_.hasName(vname)) match {
          case Some(t: TimeMl) if t.getType == "text" =>
            val formatStr = t.getMetadataAttributes.get("units").get
            val tf = TimeFormat(formatStr)
            (arr, i) => {
              val x = arr.getObject(i).toString
              tf.parse(x).toDouble
            }
          case _ =>
            (arr, i) => arr.getDouble(i)
        }

      val index: Array[Double] = {
        val ncvar = getNcVar(vname)
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

  // Given LaTiS Variable primary name
  private def readIntoCache(vname: String): Unit = {
    //TODO: apply scale and offset
    readData(vname) match {
      case None          => cache(vname, DataSeq())
      case Some(ncarray) =>
        val n = ncarray.getSize.toInt
        //Store data based on the type of variable
        val datas = getOrigDataset.findVariableByName(vname) match {
          case Some(i: Integer) =>
            (0 until n).map(ncarray.getLong(_)).map(Data(_))
          case Some(r: Real) =>
            (0 until n).map(ncarray.getDouble(_)).map(Data(_))
          case Some(t: Text) =>
            (0 until n).map(ncarray.getObject(_)).map(o => Data(o.toString))
        }
        cache(vname, DataSeq(datas))
    }
  }

  /*
   * We need to set the "length" of the inner function, which is
   * defined in the TSML, to the correct value based on the selections
   * we were given. This requires reading the domain variables from
   * the NetCDF file and querying our index to figure out how much of
   * the data we'll actually be reading for the inner function. There
   * isn't a good mechanism for this at the moment.
   */
  override def makeOrigDataset: Dataset = {
    val ds = super.makeOrigDataset

    // Build indices for domain variable with Selections.
    operations.collect { case s: Selection => s }
      .map(_.vname).distinct.foreach(buildIndex(_))

    // Apply the Operations that we agreed to handle.
    // Build up map of ranges.
    applyOperations

    // Update nested Function length in metadata.
    //
    // TODO: Reconcile with lengthOfFirstDomainDimension
    val rs = ranges.toSeq.tail.collect {
      case (_, Some(r)) => r.length
      case (k, None)    => getNcVar(k).getSize.toInt
    }
    replaceLengthMetadata(ds, rs)
  }

  def replaceLengthMetadata(ds: Dataset, rs: Seq[Int]): Dataset = {
    def go(g: Function, rs: Seq[Int]): Function = {
      val md = if (rs.head > 0) {
        g.getMetadata + (("length", s"${rs.head}"))
      } else {
        g.getMetadata
      }
      g.getRange match {
        case h: Function => Function(g.getDomain, go(h, rs.tail), md)
        case range       => Function(g.getDomain, range, md)
      }
    }

    ds match {
      case Dataset(f: Function) =>
        f.getRange match {
          case g: Function =>
            Dataset(
              Function(f.getDomain, go(g, rs), f.getMetadata),
              ds.getMetadata
            )
          case _ => ds
        }
    }
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
      case s @ Selection(vname, _, _) =>
        indexMap.get(vname).foreach { index =>
          queryIndex(index, s) match {
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
        val range = new URange(n)
        ranges.headOption.foreach {
          case (k, None)    => ranges += k -> Option(range)
          case (k, Some(v)) => ranges += k -> Option(v compose range)
        }
      case op: TakeOperation =>
        val range = new URange(op.n)
        ranges.headOption.foreach {
          case (k, None)    => ranges += k -> Option(range)
          case (k, Some(v)) => ranges += k -> Option(v compose range)
        }
      case op: TakeRightOperation =>
        val range = {
          val len = lengthOfFirstDomainDimension
          new URange(len - op.n, op.n)
        }
        ranges.headOption.foreach {
          case (k, None)    => ranges += k -> Option(range)
          case (k, Some(v)) => ranges += k -> Option(v compose range)
        }
      case StrideFilter(n) =>
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

  /**
   * Read all the data into cache using Sections from the Operations.
   */
  override def init = getOrigScalars.foreach(s => readIntoCache(s.getName))

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
          case "="  => Option(new URange(i, i))
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
          case "="        => None
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
  
  /**
   * Return a sequence of VariableMl corresponding to the domain
   * variables for this DatasetMl. 
   */
  def getDomainVars(ds: DatasetMl): Seq[VariableMl] = {
    def go(vml: VariableMl, acc: Seq[VariableMl]): Seq[VariableMl] = {
      vml match {
        case f: FunctionMl => go(f.range, acc :+ f.domain)  //TODO: consider tuple domain
        case t: TupleMl => t.variables.map(go(_,acc)).flatten
        case _ => acc
      }
    }
    
    go(ds.getVariableMl, Seq.empty)
  }
}
