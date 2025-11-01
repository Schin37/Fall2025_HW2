package ragPipeline.helper

import org.apache.hadoop.io.BytesWritable
import org.apache.pdfbox.io.MemoryUsageSetting
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper

import java.io.ByteArrayInputStream

// Extracts texts from pdfs
object Pdf {
  // Remove text from pdfs
  def readTextBytes(bw: BytesWritable): String = {
    // Use only the valid portion of the backing array
    val in = new ByteArrayInputStream(bw.getBytes, 0, bw.getLength)
    val mem = MemoryUsageSetting.setupTempFileOnly() // spill large PDFs to temp

    val doc = PDDocument.load(in, mem)
    try new PDFTextStripper().getText(doc)
    finally doc.close()
  }
}


