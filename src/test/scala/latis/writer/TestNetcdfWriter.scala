package latis.writer

import org.junit._
import Assert._
import latis.dm._
import latis.metadata._
import java.io.File
import latis.reader.DatasetAccessor


class TestNetcdfWriter {

  @Test
  def simple_time_series(): Unit = {
    // Define test dataset: time -> (a, b)
    val samples = List(
      Sample(Real(Metadata("time"), 0), Tuple(Real(Metadata("a"), 0), Real(Metadata("b"), 1.1))), 
      Sample(Real(Metadata("time"), 1), Tuple(Real(Metadata("a"), 1), Real(Metadata("b"), 2.2))), 
      Sample(Real(Metadata("time"), 2), Tuple(Real(Metadata("a"), 2), Real(Metadata("b"), 3.3)))
    )
    val ds = Dataset(Function(samples))
    
    // Write test NetCDF file
    val file = new File("/tmp/simple_time_series.nc")
    NetcdfWriter().writeFile(ds, file)
    
    // Read test NetCDF file
    val ds2 = DatasetAccessor.fromName("simple_time_series").getDataset()
    ds2 match {
      case Dataset(Function(it)) => it.next match {
        case Sample(Real(t), TupleMatch(Real(a), Real(b))) =>
          assertEquals(0, t, 0)
          assertEquals(0, a, 0)
          assertEquals(1.1, b, 0)
      }
    }
    
    // Delete test file
    val z = file.delete()
  }

}
