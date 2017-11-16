package latis.reader.tsml

import java.io.{File, FileNotFoundException}

import scala.collection.Searching._
import scala.collection.mutable
import scala.util.Try

import ucar.ma2.{Array => NCArray}
import ucar.ma2.{Range => URange}
import ucar.ma2.Section
import ucar.nc2.NetcdfFile
import ucar.nc2.dataset.NetcdfDataset
import ucar.nc2.util.EscapeStrings

import latis.data.Data
import latis.data.seq.DataSeq
import latis.dm._
import latis.ops.Operation
import latis.ops.filter._
import latis.reader.tsml.ml._
import latis.time._
import latis.util.StringUtils

import IterativeNetcdfAdapter._

class IterativeNetcdfAdapter(tsml: Tsml) extends IterativeAdapter2[Map[String, Data]](tsml) {

  private lazy val ncFile: NetcdfFile = {
    val location = getUrl.toString
    Try(NetcdfDataset.openFile(location, null)).recover {
      case _: FileNotFoundException =>
        logger.warn("First attempt failed to open " + location)

        // See LISIRDIII-917.
        val path = getUrl.getPath
        val basename = path.substring(0, path.lastIndexOf(File.separator))
        val baseDir = new File(basename)
        baseDir.listFiles()

        // Try one more time. The 'get' below will cause an exception
        // to be thrown if this fails again.
        NetcdfDataset.openFile(location, null)
    }.get
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
    val domainVar = domainVars.find(_.hasName(vname)).get
    val units = domainVar.getMetadataAttributes.get("units").getOrElse {
      val msg = "Time variable must have units."
      throw new UnsupportedOperationException(msg)
    }
    val ts = TimeScale(units)
    Time.fromIso(value).convert(ts).getValue.toString
  }

  // Given LaTiS Variable primary name
  private def getNcVar(vname: String): Option[ucar.nc2.Variable] = {
    val name = {
      val oname = tsml.findVariableAttribute(vname, "origName")
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
  private def buildIndex(vname: String): Unit = getNcVar(vname) match {
    case Some(ncvar) if (!indexMap.isDefinedAt(vname)) =>
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

  /*
   * We need to set the "length" of the inner function, which is
   * defined in the TSML, to the correct value based on the selections
   * we were given. This requires reading the domain variables from
   * the NetCDF file and querying our index to figure out how much of
   * the data we'll actually be reading for the inner function. There
   * isn't a good mechanism for this at the moment.
   */
  override def makeOrigDataset: Dataset = {
    // If this adapter is being used in template mode, the path will
    // contain "..." and otherwise we assume it won't.
    val location = getUrl.toString
    if (location.contains("...")) {
      return super.makeOrigDataset
    }

    val ds = super.makeOrigDataset

    // Build indices for domain variables that have some sort of
    // selection on them.
    operations.collect {
      case Selection(vname, _, _)          => vname
      case NearestNeighborFilter(vname, _) => vname
    }.distinct.foreach(buildIndex(_))

    // Apply the Operations that we agreed to handle.
    // Build up map of ranges.
    applyOperations

    // Update nested Function length in metadata.
    // TODO: Reconcile with lengthOfFirstDomainDimension
    val rs = ranges.toSeq.collect {
      case (_, Some(r)) => r.length
      case (k, None)    => getNcVar(k) match {
        case Some(ncvar) => ncvar.getSize.toInt
        // If the variable is not in the file, get from the tsml
        case None => ds.findVariableByName(k) match {
          case Some(v) => v.getMetadata("length") match {
            case Some(s) => s.toInt //TODO: parse error
            case None => ??? //TODO: error: length not defined for derived variable
          }
          case None => ??? //TODO: error: variable not in tsml, shouldn't happen
        }
      }
    }
    replaceLengthMetadata(ds, rs)
  }

  def replaceLengthMetadata(ds: Dataset, rs: Seq[Int]): Dataset = {
    def go(v: Variable, rs: Seq[Int]): Variable = v match {
      case f: Function =>
        val md = if (rs.head > 0) {
          f.getMetadata + ("length", s"${rs.head}")
        } else {
          f.getMetadata
        }

        Function(f.getDomain, go(f.getRange, rs.tail), md)
      case t @ Tuple(vs) => Tuple(vs.map(v => go(v, rs)), t.getMetadata)
      case _ => v
    }

    ds match {
      case Dataset(v) => Dataset(go(v, rs), ds.getMetadata)
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

  override def getRecordIterator: Iterator[Map[String, Data]] = {
    val sections = getOrigScalars.map { scalar =>
      (scalar, makeSection(scalar.getName))
    }
    new NetcdfIterator(ncFile, sections)
  }

  override def parseRecord(record: Map[String, Data]): Option[Map[String, Data]] =
    Option(record)

  override def close: Unit = ncFile.close()
}

object IterativeNetcdfAdapter {
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

  /**
   * Return a sequence of VariableMl corresponding to the domain
   * variables for this DatasetMl.
   */
  def getDomainVars(ds: DatasetMl): Seq[VariableMl] = {
    def go(vml: VariableMl, acc: Seq[VariableMl]): Seq[VariableMl] = {
      vml match {
        case f: FunctionMl => f.domain match {
          // TODO: I don't think this will work for nested tuples.
          case t: TupleMl => go(f.range, acc ++ t.variables)
          case _ => go(f.range, acc :+ f.domain)
        }
        case t: TupleMl => acc ++ t.variables.map(go(_, Seq())).flatten
        case _ => acc
      }
    }

    go(ds.getVariableMl, Seq.empty)
  }
}

class NetcdfIterator(ncFile: NetcdfFile, sections: Seq[(Scalar, Option[Section])])
    extends Iterator[Map[String, Data]] {

  // If we aren't filtering or selecting on the outer dimension,
  // 'getRange' returns null, so we need to figure out how long the
  // outer dimension is and construct the range ourselves.
  private val outerRange: URange = sections.head match {
    case (_, Some(s)) => Option(s.getRange(0)).getOrElse {
      val size = ncFile.findVariable(sections.head._1.getName).getSize()
      new URange(size.toInt)
    }
    case (_, None)    => ???
  }

  private var position: Int = 0

  private var internalIterator: Iterator[Map[String, Data]] =
    Iterator.empty

  private def readData(scalar: Scalar, section: Section): NCArray = {
    val ncVar = {
      val name = scalar.getMetadata("origName").getOrElse(scalar.getName)
      ncFile.findVariable(name)
    }
    ncVar.read(section)
  }

  private def readNetcdfRecord(pos: Int): Seq[Map[String, Data]] = {
    // Assuming SSI

    val time = sections(0)
    val wavelength = sections(1)
    val irradiance = sections(2)

    // Read the wavelength first so we know the length of the inner
    // dimension. We can't always tell from the section.
    val wData = wavelength match {
      case (scalar, None)    => (scalar.getName, DataSeq())
      case (scalar, Some(s)) =>
        val ncArr = readData(scalar, s)
        val n = ncArr.getSize.toInt

        val data = scalar match {
          case _: Integer =>
            DataSeq((0 until n).map(ncArr.getLong(_)).map(Data(_)))
          case _: Real    =>
            DataSeq((0 until n).map(ncArr.getDouble(_)).map(Data(_)))
          case _: Text    =>
            DataSeq((0 until n).map(ncArr.getObject(_)).map(o => Data(o.toString)))
        }

        (scalar.getName, data)
    }

    val iData = irradiance match {
      case (scalar, None)    => (scalar.getName, DataSeq())
      case (scalar, Some(s)) =>
        val recordSection = {
          val p = outerRange.element(pos)
          s.replaceRange(0, new URange(p, p))
        }

        val ncArr = readData(scalar, recordSection)
        val n = ncArr.getSize.toInt

        val data = scalar match {
          case _: Integer =>
            DataSeq((0 until n).map(ncArr.getLong(_)).map(Data(_)))
          case _: Real    =>
            DataSeq((0 until n).map(ncArr.getDouble(_)).map(Data(_)))
          case _: Text    =>
            DataSeq((0 until n).map(ncArr.getObject(_)).map(o => Data(o.toString)))
        }

        (scalar.getName, data)
    }

    val tData = time match {
      case (scalar, None) => (scalar.getName, DataSeq())
      case (scalar, Some(s)) =>
        val recordSection = {
          val p = outerRange.element(pos)
          s.replaceRange(0, new URange(p, p))
        }

        val ncArr = readData(scalar, recordSection)
        val n = wData._2.length

        val data = scalar match {
          case _: Integer =>
            DataSeq(Seq.fill(n)(Data(ncArr.getLong(0))))
          case _: Real    =>
            DataSeq(Seq.fill(n)(Data(ncArr.getDouble(0))))
          case _: Text    =>
            DataSeq(Seq.fill(n)(Data(ncArr.getObject(0).toString)))
        }

        (scalar.getName, data)
    }

    Seq(tData, wData, iData).map {
      case (name, xs) => xs.iterator.toSeq.map((name, _))
    }.transpose.map(_.toMap)
  }

  override def hasNext: Boolean =
    (position < outerRange.length) || internalIterator.hasNext


  override def next: Map[String, Data] =
    if (internalIterator.hasNext) {
      internalIterator.next
    } else {
      val record = readNetcdfRecord(position)
      position += 1

      internalIterator = record.iterator
      next
    }
}
