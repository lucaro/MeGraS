package org.megras.segmentation.type

import org.davidmoten.hilbert.HilbertCurve
import org.megras.segmentation.SegmentationClass
import org.megras.segmentation.SegmentationType
import kotlin.math.pow
import kotlin.math.roundToLong

data class Interval<T : Number>(val low: T, val high: T)

abstract class OneDimensionalSegmentation : Segmentation {
    abstract val intervals: List<Interval<*>>

    override fun equivalentTo(rhs: Segmentation): Boolean {
        return this.contains(rhs) && rhs.contains(this)
    }

    override fun contains(rhs: Segmentation): Boolean {
        if (rhs !is OneDimensionalSegmentation) return false

        // All rhs intervals are contained in some intervals
        return rhs.intervals.all { j ->
            this.intervals.any { i -> i.low <= j.low && j.high <= i.high }
        }
    }

    override fun intersects(rhs: Segmentation): Boolean {
        if (rhs !is OneDimensionalSegmentation) return false

        // Any two intervals overlap
        return this.intervals.any { i ->
            rhs.intervals.any { j -> i.low <= j.high && j.low <= i.high }
        }
    }
}

private operator fun Number.compareTo(low: Number): Int {
    if (this is Int) return this.compareTo(low.toInt())
    if (this is Double) return this.compareTo(low.toDouble())
    if (this is Long) return this.compareTo(low.toLong())
    return this.toDouble().compareTo(low.toDouble())
}

class Hilbert(val dimensions: Int, val order: Int, override var intervals: List<Interval<Long>>) : OneDimensionalSegmentation() {
    override val segmentationType: SegmentationType = SegmentationType.HILBERT
    override val segmentationClass: SegmentationClass
    private val hilbertCurve = HilbertCurve.small().bits(order).dimensions(dimensions)
    private val dimensionSize = (2.0).pow(order) - 1

    var relativeTimestamp: Double? = null
        set(value) {
            field = value
        }

    init {
        segmentationClass = when (dimensions) {
            2 -> SegmentationClass.SPACE
            3 -> SegmentationClass.SPACETIME
            else -> throw IllegalArgumentException("Dimension not supported.")
        }

        require(intervals.all { it.low <= it.high }) {
            throw IllegalArgumentException("Ranges are not valid.")
        }

        require(intervals.all { it.high <= hilbertCurve.maxIndex() }) {
            throw IllegalArgumentException("Range is out of bounds.")
        }
    }

    fun isIncluded(vararg relativeCoords: Double): Boolean {

        // Translate to hilbert space
        val hilbertCoords = relativeCoords.map { (it * dimensionSize).roundToLong() }.toMutableList()

        if (relativeTimestamp != null) {
            hilbertCoords.add((relativeTimestamp!! * dimensionSize).roundToLong())
        }

        val hilbertIndex = hilbertCurve.index(*hilbertCoords.toLongArray())
        val found = intervals.find { i -> i.low <= hilbertIndex && hilbertIndex <= i.high }

        return found != null
    }

    override fun toString(): String = "segment/hilbert/${dimensions},${order}," + intervals.joinToString(",") {"${it.low}-${it.high}"}
}

class Time(override var intervals: List<Interval<Double>>) : OneDimensionalSegmentation(), Translatable {
    override val segmentationType: SegmentationType = SegmentationType.TIME
    override val segmentationClass = SegmentationClass.TIME

    override fun translate(by: Segmentation) {
        if (by is Time) {
            val shift = by.intervals[0].low
            intervals = intervals.map { Interval(it.low + shift, it.high + shift) }
        }
    }

    fun getIntervalsToDiscard(): List<Interval<Double>> {
        val newIntervals = mutableListOf<Interval<Double>>()

        for (i in 0 until intervals.size - 1) {
            newIntervals.add(Interval(intervals[i].high, intervals[i + 1].low))
        }
        return newIntervals
    }

    override fun toString(): String = "segment/time/" + intervals.joinToString(",") { "${it.low}-${it.high}" }
}

class Page(override var intervals: List<Interval<Int>>) : OneDimensionalSegmentation(), Translatable {
    override val segmentationType = SegmentationType.PAGE
    override val segmentationClass = SegmentationClass.TIME

    override fun translate(by: Segmentation) {
        if (by is Page) {
            val shift = by.intervals[0].low
            intervals = intervals.map { Interval(it.low + shift, it.high + shift) }
        }
    }

    override fun toString(): String = "segment/page/" + intervals.joinToString(",") { "${it.low}-${it.high}" }
}