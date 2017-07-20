
import org.junit._
import Assert._

import latis.ops.filter.Selection
import latis.reader.tsml._
import latis.reader.tsml.NetcdfAdapter4._
import java.net.URL
import latis.ops.Operation
import latis.ops.filter.FirstFilter
import latis.ops.filter.TakeOperation
import latis.ops.filter.TakeRightOperation
import latis.ops.TimeFormatter
import latis.ops.filter.LastFilter
import latis.ops.Projection

// TODO: This is an excellent candidate for property-based tests.

class TestNetcdfAdapter4 {
  
  @Test
  def nrl2_ssi_P1Y = {
    val reader = TsmlReader2(new URL("file:/home/lindholm/git/latis-netcdf/src/test/resources/datasets/nrl2_ssi_P1Y.tsml"))
    
    //val m = reader.model
    //val v = m.findVariableByName("time")
    //val z = m.findVariableAttribute("time", "shape")
    //println(z)
    
    val ops = scala.collection.mutable.ArrayBuffer[Operation]()
    //ops += FirstFilter()
    //ops += LastFilter()
    ops += TakeOperation(3)
    ops += Selection("wavelength<120")
    ops += TimeFormatter("yyyy-MM-dd")
    val ds = reader.getDataset(ops)
    latis.writer.Writer.fromSuffix("asc").write(ds)
  }
  
  @Test
  def timed_see_ssi_l3 = {
    val reader = TsmlReader2(new URL("file:/home/lindholm/git/latis-netcdf/src/test/resources/datasets/timed_see_ssi_l3.tsml"))
    
    val ops = scala.collection.mutable.ArrayBuffer[Operation]()
    ops += FirstFilter()
    ops += Selection("wavelength<5")
    ops += Projection("time,wavelength,irradiance")
    val ds = reader.getDataset(ops)
    latis.writer.Writer.fromSuffix("asc").write(ds)
  }
  
  @Test
  def timed_see_xps_diodes_l3a = {
    val reader = TsmlReader2(new URL("file:/home/lindholm/git/latis-netcdf/src/test/resources/datasets/timed_see_xps_diodes_l3a.tsml"))
    val ops = scala.collection.mutable.ArrayBuffer[Operation]()
    ops += Selection("time > 2002-01-22T12")
    ops += TakeOperation(3)
    //ops += Projection("time,diode9")
    ops += TimeFormatter("yyyy-MM-dd HH:mm")
    val ds = reader.getDataset(ops)
    latis.writer.Writer.fromSuffix("asc").write(ds)
  }
}
