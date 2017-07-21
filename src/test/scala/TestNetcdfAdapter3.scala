package latis.reader.tsml

import org.junit._
import Assert._

import NetcdfAdapter3._

// TODO: This is an excellent candidate for property-based tests.

class TestNetcdfAdapter3 {

  val index: Array[Double] = Array(2, 4, 6, 8)

  @Test
  def empty = {
    val arr: Array[Double] = Array()
    val r = queryIndex(arr, ">", 0)

    assertTrue(r.isEmpty)
  }

  @Test
  def gt = {
    val r = queryIndex(index, ">", 4)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(2, 3), _))
  }

  @Test
  def ge = {
    val r = queryIndex(index, ">=", 4)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(1, 3), _))
  }

  @Test
  def lt = {
    val r = queryIndex(index, "<", 4)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 0), _))
  }

  @Test
  def le = {
    val r = queryIndex(index, "<=", 4)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 1), _))
  }

  @Test
  def nearest = {
    val r = queryIndex(index, "~", 4)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(1, 1), _))
  }

  @Test
  def gt_nonexistant = {
    val r = queryIndex(index, ">", 5)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(2, 3), _))
  }

  @Test
  def ge_nonexistant = {
    val r = queryIndex(index, ">=", 5)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(2, 3), _))
  }

  @Test
  def lt_nonexistant = {
    val r = queryIndex(index, "<", 5)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 1), _))
  }

  @Test
  def le_nonexistant = {
    val r = queryIndex(index, "<=", 5)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 1), _))
  }

  @Test
  def nearest_nonexistant = {
    val r = queryIndex(index, "~", 4.1)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(1, 1), _))
  }

  @Test
  def gt_end = {
    val r = queryIndex(index, ">", 8)

    assertTrue(r.isEmpty)
  }

  @Test
  def ge_end = {
    val r = queryIndex(index, ">=", 8)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(3, 3), _))
  }

  @Test
  def lt_end = {
    val r = queryIndex(index, "<", 8)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 2), _))
  }

  @Test
  def le_end = {
    val r = queryIndex(index, "<=", 8)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 3), _))
  }

  @Test
  def nearest_end = {
    val r = queryIndex(index, "~", 8)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(3, 3), _))
  }

  @Test
  def gt_beg = {
    val r = queryIndex(index, ">", 2)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(1, 3), _))
  }

  @Test
  def ge_beg = {
    val r = queryIndex(index, ">=", 2)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 3), _))
  }

  @Test
  def lt_beg = {
    val r = queryIndex(index, "<", 2)

    assertTrue(r.isEmpty)
  }

  @Test
  def le_beg = {
    val r = queryIndex(index, "<=", 2)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 0), _))
  }

  @Test
  def nearest_beg = {
    val r = queryIndex(index, "~", 2)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 0), _))
  }

  @Test
  def le_beg_nonexistant = {
    val r = queryIndex(index, "<=", 1)

    assertTrue(r.isEmpty)
  }

  @Test
  def lt_beg_nonexistant = {
    val r = queryIndex(index, "<", 1)

    assertTrue(r.isEmpty)
  }

  @Test
  def ge_beg_nonexistant = {
    val r = queryIndex(index, ">=", 1)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 3), _))
  }

  @Test
  def gt_beg_nonexistant = {
    val r = queryIndex(index, ">", 1)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 3), _))
  }

  @Test
  def nearest_beg_nonexistant = {
    val r = queryIndex(index, "~", 1)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 0), _))
  }

  @Test
  def le_end_nonexistant = {
    val r = queryIndex(index, "<=", 9)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 3), _))
  }

  @Test
  def lt_end_nonexistant = {
    val r = queryIndex(index, "<", 9)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 3), _))
  }

  @Test
  def ge_end_nonexistant = {
    val r = queryIndex(index, ">=", 9)

    assertTrue(r.isEmpty)
  }

  @Test
  def gt_end_nonexistant = {
    val r = queryIndex(index, ">", 9)

    assertTrue(r.isEmpty)
  }

  @Test
  def nearest_end_nonexistant = {
    val r = queryIndex(index, "~", 9)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(3, 3), _))
  }

  @Test
  def nearest_equidistant = {
    val r = queryIndex(index, "~", 3)

    assertTrue(r.isDefined)
    // We expect to round down to 2 rather than round up to 4.
    r.foreach(assertEquals(new ucar.ma2.Range(0, 0), _))
  }

  @Test
  def nearest_a_nearest = {
    // name ~ 2.1
    val r = queryIndex(index, "~", 2.1)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 0), _))
  }

  @Test
  def nearest_b_nearest = {
    val r = queryIndex(index, "~", 3.9)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(1, 1), _))
  }
}
