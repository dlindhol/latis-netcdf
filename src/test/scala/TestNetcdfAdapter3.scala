package latis.reader.tsml

import org.junit._
import Assert._

import latis.ops.filter.Selection
import NetcdfAdapter3._

// TODO: This is an excellent candidate for property-based tests.

class TestNetcdfAdapter3 {

  val index: Array[Double] = Array(2, 4, 6, 8)

  @Test
  def empty = {
    val arr: Array[Double] = Array()
    val s = new Selection("", ">", "0")
    val r = queryIndex(arr, s)

    assertTrue(r.isEmpty)
  }

  @Test
  def gt = {
    val s = new Selection("", ">", "4")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(2, 3), _))
  }

  @Test
  def ge = {
    val s = new Selection("", ">=", "4")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(1, 3), _))
  }

  @Test
  def lt = {
    val s = new Selection("", "<", "4")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 0), _))
  }

  @Test
  def le = {
    val s = new Selection("", "<=", "4")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 1), _))
  }

  @Test
  def gt_nonexistant = {
    val s = new Selection("", ">", "5")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(2, 3), _))
  }

  @Test
  def ge_nonexistant = {
    val s = new Selection("", ">=", "5")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(2, 3), _))
  }

  @Test
  def lt_nonexistant = {
    val s = new Selection("", "<", "5")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 1), _))
  }

  @Test
  def le_nonexistant = {
    val s = new Selection("", "<=", "5")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 1), _))
  }

  @Test
  def gt_end = {
    val s = new Selection("", ">", "8")
    val r = queryIndex(index, s)

    assertTrue(r.isEmpty)
  }

  @Test
  def ge_end = {
    val s = new Selection("", ">=", "8")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(3, 3), _))
  }

  @Test
  def lt_end = {
    val s = new Selection("", "<", "8")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 2), _))
  }

  @Test
  def le_end = {
    val s = new Selection("", "<=", "8")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 3), _))
  }

  @Test
  def gt_beg = {
    val s = new Selection("", ">", "2")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(1, 3), _))
  }

  @Test
  def ge_beg = {
    val s = new Selection("", ">=", "2")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 3), _))
  }

  @Test
  def lt_beg = {
    val s = new Selection("", "<", "2")
    val r = queryIndex(index, s)

    assertTrue(r.isEmpty)
  }

  @Test
  def le_beg = {
    val s = new Selection("", "<=", "2")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 0), _))
  }

  @Test
  def le_beg_nonexistant = {
    val s = new Selection("", "<=", "1")
    val r = queryIndex(index, s)

    assertTrue(r.isEmpty)
  }

  @Test
  def lt_beg_nonexistant = {
    val s = new Selection("", "<", "1")
    val r = queryIndex(index, s)

    assertTrue(r.isEmpty)
  }

  @Test
  def ge_beg_nonexistant = {
    val s = new Selection("", ">=", "1")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 3), _))
  }

  @Test
  def gt_beg_nonexistant = {
    val s = new Selection("", ">", "1")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 3), _))
  }

  @Test
  def le_end_nonexistant = {
    val s = new Selection("", "<=", "9")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 3), _))
  }

  @Test
  def lt_end_nonexistant = {
    val s = new Selection("", "<", "9")
    val r = queryIndex(index, s)

    assertTrue(r.isDefined)
    r.foreach(assertEquals(new ucar.ma2.Range(0, 3), _))
  }

  @Test
  def ge_end_nonexistant = {
    val s = new Selection("", ">=", "9")
    val r = queryIndex(index, s)

    assertTrue(r.isEmpty)
  }

  @Test
  def gt_end_nonexistant = {
    val s = new Selection("", ">", "9")
    val r = queryIndex(index, s)

    assertTrue(r.isEmpty)
  }
}
