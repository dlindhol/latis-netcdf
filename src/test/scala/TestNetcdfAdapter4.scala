import org.junit._
import Assert._

import latis.ops.filter.Selection
import latis.reader.tsml._
import latis.reader.adapter.NetcdfAdapter4._
import java.net.URL
import latis.ops.Operation
import latis.ops.filter.FirstFilter
import latis.ops.filter.TakeOperation
import latis.ops.filter.TakeRightOperation
import latis.ops.TimeFormatter
import latis.ops.filter.LastFilter
import latis.ops.Projection
import latis.dm._

class TestNetcdfAdapter4 {
  
  @Test
  def nrl2_ssi_P1Y = {
    val reader = TsmlReader2.fromName("nrl2_ssi_P1Y")
    
    //val m = reader.model
    //val v = m.findVariableByName("time")
    //val z = m.findVariableAttribute("time", "shape")
    //println(z)
    
    val ops = scala.collection.mutable.ArrayBuffer[Operation]()
    //ops += FirstFilter()
    //ops += LastFilter()
    ops += TakeOperation(3)
    ops += Selection("wavelength~119.6")
    //ops += TimeFormatter("yyyy")
    val ds = reader.getDataset(ops)
    //latis.writer.Writer.fromSuffix("asc").write(ds)
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(Number(year), r) => 
          assertEquals(181.0, year, 0)
          r match {
            case Function(it) => it.next match {
              case Sample(Real(wl), Real(ir)) =>
                assertEquals(119.5, wl, 0)
                assertEquals(5.220650928094983E-5, ir, 0)
            }
          }
      }
    }
  }
  
  @Test
  def timed_see_ssi_l3 = {
    val reader = TsmlReader2.fromName("timed_see_ssi_l3")
    
    val ops = scala.collection.mutable.ArrayBuffer[Operation]()
    ops += FirstFilter()
    ops += Selection("wavelength<5")
    ops += Projection("time,wavelength,irradiance")
    val ds = reader.getDataset(ops)
    //latis.writer.Writer.fromSuffix("asc").write(ds)
    ds match {
      case Dataset(Function(it)) => it.next match {
        case Sample(Text(time), r) => 
          assertEquals("2002022", time)
          r match {
            case Function(it) => it.next match {
              case Sample(Real(wl), Real(ir)) =>
                assertEquals(0.5, wl, 0)
                assertEquals(-1.0, ir, 0)
            }
          }
      }
    }
  }
}
