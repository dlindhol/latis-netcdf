package latis.writer

import latis.dm._
import java.io.File
import edu.ucar.ral.nujan.netcdf._
import edu.ucar.ral.nujan.netcdf.NhFileWriter._
import edu.ucar.ral.nujan.netcdf.NhVariable._

/**
 * This writer will read the entire dataset into memory then write 
 * a NetCDF file that will be streamed to the provided output.
 * All variables are assumed to be doubles.
 * It is currently limited to time series datasets with no 
 * nested functions (e.g. spectra).
 *   time -> (a, b, c, ...)
 */
class NetcdfWriter extends FileWriter {
  
  def writeFile(dataset: Dataset, file: File): Unit = {
    
    // Read all data into an immutable Map with 
    //  variable name mapped to Array[Double]
    val dataMap = dataset.toDoubleMap
    
    // Number of time samples
    val ntim = dataMap.get("time") match {
      case Some(a) => a.length
      case None => throw new RuntimeException("No time variable found in dataset.")
    }
    
    // Create the NetCDF file
    val ncWriter = new NhFileWriter(file.getPath, OPT_OVERWRITE)
    val rootGroup = ncWriter.getRootGroup
    
    // Add global metadata
    dataset.getMetadata.getProperties map { case (key, value) =>
      rootGroup.addAttribute(key, TP_STRING_VAR, value)
    }
  
    // Define dimensions
    val tdim = rootGroup.addDimension("time", ntim) //Note: unlimited not supported
  
    // Define variables
    val vars: Seq[Scalar] = dataset.getScalars
    val nhvars: Seq[NhVariable] = vars map { v =>
      val nhvar = rootGroup.addVariable(v.getName, TP_DOUBLE, Array(tdim), null, null, 0)
      // Add variable metadata attributes
      v.getMetadata.getProperties map { case (key, value) =>
        nhvar.addAttribute(key, TP_STRING_VAR, value)
      }
      nhvar
    }
  
    // Complete NetCDF file definition
    ncWriter.endDefine()
  
    // Write data to NetCDF file
    (vars zip nhvars) foreach { case (v, nv) =>
      nv.writeData(null, dataMap(v.getName))
    }
  
    // Close the write
    ncWriter.close()  
  }
}

object NetcdfWriter {
  
  def apply(): NetcdfWriter = new NetcdfWriter()
}
