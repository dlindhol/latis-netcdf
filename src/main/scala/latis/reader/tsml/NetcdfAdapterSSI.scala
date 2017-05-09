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
import ucar.ma2.Range
import ucar.ma2.Section
import ucar.nc2.NetcdfFile
import ucar.nc2.dataset.NetcdfDataset
import ucar.nc2.util.EscapeStrings

import NetcdfAdapterSSI._

/**
 * This NetCDF adapter will read domain variables into memory (if they
 * are subject to a Selection) and use a binary search to determine
 * what subset of the data should be read from the NetCDF file.
 *
 * This adapter makes the assumption that the data is modeled as:
 *
 * A -> B -> C
 */
class NetcdfAdapterSSI(tsml: Tsml) extends TsmlAdapter(tsml) {

  private var ncFile: NetcdfFile = null

  private lazy val domainVars: Seq[VariableMl] =
    getDomainVars(tsml.dataset)

  /**
   * A map from the name of a variable to the index for that variable.
   */
  private val indexMap: mutable.Map[String, Array[Double]] =
    mutable.Map()

  private val operations: mutable.ArrayBuffer[Operation] =
    mutable.ArrayBuffer()

  /**
   * A map from the name of a variable to the range for that variable.
   */
  private val ranges: mutable.Map[String, ucar.ma2.Range] =
    mutable.Map()

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
      case _ => false
    }

  private def getNcVar(name: String): ucar.nc2.Variable = {
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

  private def buildIndex(vname: String): Unit =
    if (! indexMap.isDefinedAt(vname)) {
      val name: String = {
        val origName = for {
          variable <- domainVars.find(_.hasName(vname))
          name     <- variable.getAttribute("origName")
        } yield name
        origName.getOrElse(vname)
      }

      val index: Array[Double] = {
        val ncvar = getNcVar(name)
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

  private def readIntoCache(name: String, section: Option[Section]): Unit =
    section match {
      case None    => cache(name, DataSeq())
      case Some(s) =>
        val vname = tsml.findVariableAttribute(name, "origName")
        val ncvar = getNcVar(vname.getOrElse(name))

        val scale = getScaleFactor(ncvar)
        val ncarray = readData(ncvar, s.toString)
        val n = ncarray.getSize.toInt
        val datas = (0 until n).map(ncarray.getDouble(_) * scale).map(Data(_))
        cache(name, DataSeq(datas))
    }

  override def init = {
    getOrigDataset match {
      case Dataset(f: Function) =>
        val fd = f.getDomain
        val section = ranges.get(fd.getName) match {
          case Some(r) => Option(new Section(r))
          case None    => if (! indexMap.isDefinedAt(fd.getName)) {
            Option(new Section(":"))
          } else {
            None
          }
        }
        readIntoCache(fd.getName, section)

        f.getRange match {
          case g: Function =>
            val gd = g.getDomain
            val section = ranges.get(gd.getName) match {
              case Some(r) => Option(new Section(r))
              case None    => if (! indexMap.isDefinedAt(gd.getName)) {
                Option(new Section(":"))
              } else {
                None
              }
            }
            readIntoCache(gd.getName, section)

            val v = g.getRange
            val section2 = for {
              range1 <- ranges.get(fd.getName) match {
                case Some(r) => Option(new Section(r))
                case None    => if (! indexMap.isDefinedAt(fd.getName)) {
                  Option(new Section(":"))
                } else {
                  None
                }
              }
              range2 <- ranges.get(gd.getName) match {
                case Some(r) => Option(new Section(r))
                case None    => if (! indexMap.isDefinedAt(gd.getName)) {
                  Option(new Section(":"))
                } else {
                  None
                }
              }
            } yield new Section(s"${range1},${range2}")
            readIntoCache(v.getName, section2)
        }
    }
  }

  // We need to set the "length" of the inner function, which is
  // defined in the TSML, to the correct value based on the selections
  // we were given. This requires reading the domain variables from
  // the NetCDF file and querying our index to figure out how much of
  // the data we'll actually be reading for the inner function. There
  // isn't a good mechanism for this at the moment.
  override def makeOrigDataset: Dataset = {
    val location = getUrl.toString

    // This is a hack to get around the fact that we need to read the
    // NetCDF file in order to put accurate metadata in the original
    // dataset, but the FileJoinAdapter needs to see the original
    // dataset before it fills out the template with the real location
    // of the NetCDF file.
    //
    // If this adapter is being used in template mode, the path will
    // contain "..." and otherwise we assume it won't.
    if (location.contains("...")) {
      return super.makeOrigDataset
    }

    try {
      //open the NetCDF file as defined by the location in the tsml.
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

    val selections: Seq[Selection] =
      operations.collect {
        case s: Selection => s
      }
    selections.map(_.vname).distinct.foreach(buildIndex(_))

    selections.groupBy(_.vname).foreach {
      case (name, s) =>
        indexMap.get(name).foreach { index =>
          val rs = s.flatMap(queryIndex(index, _))
          if (! rs.isEmpty) {
            val r = rs.reduce(_ intersect _)
            if (r.length > 0) {
              ranges += name -> r
            }
          }
        }
    }

    val ds = super.makeOrigDataset
    ds match {
      case Dataset(f: Function) =>
        f.getRange match {
          case g: Function =>
            val md = ranges.get(g.getDomain.getName) match {
              case Some(r) => g.getMetadata + (("length", s"${r.length}"))
              case None    => g.getMetadata
            }
            Dataset(
              Function(f.getDomain,
                Function(g.getDomain, g.getRange, md),
                f.getMetadata),
              ds.getMetadata
            )
        }
    }
  }

  def close = ncFile.close
}

object NetcdfAdapterSSI {
  // Assuming that the data are ordered in ascending order.
  def queryIndex(index: Array[Double], s: Selection): Option[Range] = {
    val len = index.length
    if (len > 0) {
      index.search(s.value.toDouble) match {
        case Found(i) => s.operation match {
          case ">"  =>
            if (i+1 < len) {
              Option(new ucar.ma2.Range(i+1, len-1))
            } else {
              None
            }
          case ">=" => Option(new ucar.ma2.Range(i, len-1))
          case "==" => Option(new ucar.ma2.Range(i, i))
          case "<=" => Option(new ucar.ma2.Range(0, i))
          case "<"  =>
            if (i-1 >= 0) {
              Option(new ucar.ma2.Range(0, i-1))
            } else {
              None
            }
        }
        case InsertionPoint(i) => s.operation match {
          case ">" | ">=" =>
            if (i < len) {
              Option(new ucar.ma2.Range(i, len-1))
            } else {
              None
            }
          case "=="       => None
          case "<" | "<=" =>
            if (i > 0) {
              Option(new ucar.ma2.Range(0, i-1))
            } else {
              None
            }
        }
      }
    } else {
      None
    }
  }

  def getDomainVars(ds: DatasetMl): Seq[VariableMl] = {
    @tailrec
    def go(f: FunctionMl, acc: Seq[VariableMl]): Seq[VariableMl] = {
      f.range match {
        case g: FunctionMl => go(g, f.domain +: acc)
        case _             => f.domain +: acc
      }
    }

    // getVariableMl will wrap 'time' and 'index' variables in
    // implicit functions, so we get FunctionMl anyway.
    ds.getVariableMl match {
      case f: FunctionMl => go(f, Seq())
      case _             => Seq()
    }
  }
}
