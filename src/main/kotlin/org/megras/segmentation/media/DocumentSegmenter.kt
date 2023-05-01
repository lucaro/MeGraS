package org.megras.segmentation.media

import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.pdmodel.PDPage
import org.apache.pdfbox.pdmodel.PDPageContentStream
import org.apache.pdfbox.pdmodel.common.PDRectangle
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject
import org.apache.pdfbox.rendering.ImageType
import org.apache.pdfbox.rendering.PDFRenderer
import org.megras.segmentation.type.Page
import org.megras.segmentation.type.Rect
import org.megras.segmentation.type.Segmentation
import org.megras.segmentation.type.TwoDimensionalSegmentation
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import java.io.InputStream
import javax.imageio.ImageIO

object DocumentSegmenter {

    fun segment(inputStream: InputStream, segmentation: Segmentation): ByteArray? {
        try {
            val pdf = PDDocument.load(inputStream)
            val newPdf = when (segmentation) {
                is Page -> segmentPages(pdf, segmentation)
                is Rect -> segmentRect(pdf, segmentation)
                is TwoDimensionalSegmentation -> segmentShape(pdf, segmentation)
                else -> null
            } ?: return null

            val out = ByteArrayOutputStream()
            newPdf.save(out)
            newPdf.close()
            pdf.close()
            return out.toByteArray()
        } catch (e: Exception) {
            //TODO log
            return null
        }
    }

    private fun segmentPages(pdf: PDDocument, page: Page): PDDocument {
        (0 until pdf.numberOfPages).reversed().forEach { pageNumber ->
            val keep = page.intervals.any { pageNumber >= it.low && pageNumber <= it.high }
            if (!keep) {
                pdf.removePage(pageNumber)
            }
        }
        return pdf
    }

    private fun segmentRect(pdf: PDDocument, rect: Rect): PDDocument {
        for (i in 0 until pdf.numberOfPages) {
            val page = pdf.getPage(i)
            page.cropBox = PDRectangle(
                rect.xmin.toFloat(), rect.ymin.toFloat(), rect.width.toFloat(), rect.height.toFloat()
            )
        }
        return pdf
    }

    private fun segmentShape(pdf: PDDocument, segmentation: TwoDimensionalSegmentation): PDDocument? {
        val newPdf = PDDocument()
        val pdfRenderer = PDFRenderer(pdf)

        for (i in 0 until pdf.numberOfPages) {
            val page = pdfRenderer.renderImage(i, 1F, ImageType.ARGB)
            val segmentedPage = ImageSegmenter.segmentShape(page, segmentation, BufferedImage.TYPE_4BYTE_ABGR) ?: return null
            val out = ByteArrayOutputStream()
            ImageIO.write(segmentedPage, "png", out)

            val newPage = PDPage(PDRectangle(segmentedPage.width.toFloat(), segmentedPage.height.toFloat()))
            newPdf.addPage(newPage)
            val img = PDImageXObject.createFromByteArray(newPdf, out.toByteArray(), "$i")
            val contentStream = PDPageContentStream(newPdf, newPage)
            contentStream.drawImage(img, 0f, 0f)
            contentStream.close()
        }
        return newPdf
    }
}