package latis.reader.tsml

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import ucar.ma2.DataType
import ucar.nc2._
import ucar.nc2.dataset._

object NetcdfAccessOrder {

  @State(Scope.Benchmark)
  class NetcdfState {
    val dataset = {
      val ds = new NetcdfDataset()

      val size = 1000
      val d1 = new Dimension("a", size)
      ds.addDimension(null, d1)
      val d2 = new Dimension("b", size)
      ds.addDimension(null, d2)

      val v = new Variable(ds, null, null, "var", DataType.DOUBLE, "a b")
      val ve = new VariableDS(null, v, true)
      ve.setValues(ve.getSize().asInstanceOf[Int], 0, 1)
      ds.addVariable(null, ve)

      ds
    }
  }
}

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
class NetcdfAccessOrder {
  import NetcdfAccessOrder.NetcdfState

  @Benchmark
  def rowAccess(state: NetcdfState): ucar.ma2.Array =
    state.dataset.readSection("var(0,:)")

  @Benchmark
  def colAccess(state: NetcdfState): ucar.ma2.Array =
    state.dataset.readSection("var(:,0)")
}
