package org.megras.graphstore.deferred

import org.megras.data.graph.QuadValue
import org.megras.graphstore.OrderSpec

data class FilterDescriptor(
    val subjects: Set<QuadValue>? = null,
    val predicates: Set<QuadValue>? = null,
    val objects: Set<QuadValue>? = null,
    val rangeFilters: List<RangeFilter> = emptyList(),
    val textFilters: List<TextFilterEntry> = emptyList(),
    val exclusionFilters: List<ExclusionFilter> = emptyList(),
    val implicitFilters: List<ImplicitFilter> = emptyList(),
    val selectivityHints: List<SelectivityHint> = emptyList(),
    val orderBy: List<OrderSpec> = emptyList(),
    val limit: Int = Int.MAX_VALUE,
    val offset: Int = 0,
    val depth: Int = 0
) {
    data class RangeFilter(
        val predicate: QuadValue,
        val min: Double?,
        val max: Double?
    )

    data class TextFilterEntry(
        val predicate: QuadValue,
        val searchText: String
    )

    data class ExclusionFilter(
        val predicate: QuadValue,
        val excludedValues: Collection<QuadValue>
    )

    data class ImplicitFilter(
        val predicate: QuadValue,
        val referenceSubject: QuadValue,
        val parameters: Map<String, Any> = emptyMap()
    )

    data class SelectivityHint(
        val predicate: QuadValue,
        val estimatedSelectivity: Double  // 0.0 = very selective, 1.0 = unselective
    )

    fun mergeFilter(
        subjects: Collection<QuadValue>?,
        predicates: Collection<QuadValue>?,
        objects: Collection<QuadValue>?
    ): FilterDescriptor {
        if (subjects == null && predicates == null && objects == null) return this
        if (subjects?.isEmpty() == true || predicates?.isEmpty() == true || objects?.isEmpty() == true) {
            return copy(
                subjects = subjects?.toSet() ?: this.subjects,
                predicates = predicates?.toSet() ?: this.predicates,
                objects = objects?.toSet() ?: this.objects,
                depth = depth + 1
            )
        }
        return copy(
            subjects = intersectSets(this.subjects, subjects?.toSet()),
            predicates = intersectSets(this.predicates, predicates?.toSet()),
            objects = intersectSets(this.objects, objects?.toSet()),
            depth = depth + 1
        )
    }

    fun withRangeFilter(predicate: QuadValue, min: Double?, max: Double?): FilterDescriptor =
        copy(rangeFilters = rangeFilters + RangeFilter(predicate, min, max), depth = depth + 1)

    fun withTextFilter(predicate: QuadValue, searchText: String): FilterDescriptor =
        copy(textFilters = textFilters + TextFilterEntry(predicate, searchText), depth = depth + 1)

    fun withExclusionFilter(predicate: QuadValue, excluded: Collection<QuadValue>): FilterDescriptor =
        copy(exclusionFilters = exclusionFilters + ExclusionFilter(predicate, excluded), depth = depth + 1)

    fun withImplicitFilter(predicate: QuadValue, referenceSubject: QuadValue, parameters: Map<String, Any> = emptyMap()): FilterDescriptor =
        copy(implicitFilters = implicitFilters + ImplicitFilter(predicate, referenceSubject, parameters), depth = depth + 1)

    fun withSelectivityHint(predicate: QuadValue, selectivity: Double): FilterDescriptor =
        copy(selectivityHints = selectivityHints + SelectivityHint(predicate, selectivity), depth = depth + 1)

    fun withOrdering(orderBy: List<OrderSpec>, limit: Int, offset: Int = 0): FilterDescriptor =
        copy(orderBy = orderBy, limit = minOf(this.limit, limit), offset = offset, depth = depth + 1)

    val isTrivial: Boolean
        get() = subjects == null && predicates == null && objects == null
                && rangeFilters.isEmpty() && textFilters.isEmpty() && exclusionFilters.isEmpty()
                && implicitFilters.isEmpty() && selectivityHints.isEmpty()
                && orderBy.isEmpty() && limit == Int.MAX_VALUE && offset == 0

    val isComposable: Boolean
        get() = depth < CIRCUIT_BREAKER_DEPTH

    val isImpossible: Boolean
        get() = subjects?.isEmpty() == true || predicates?.isEmpty() == true || objects?.isEmpty() == true

    private fun intersectSets(
        existing: Set<QuadValue>?,
        incoming: Set<QuadValue>?
    ): Set<QuadValue>? {
        if (existing == null && incoming == null) return null
        if (existing == null) return incoming
        if (incoming == null) return existing
        val result = existing.intersect(incoming)
        return result.ifEmpty { emptySet() }
    }

    fun isSqlPushDownCandidate(): Boolean {
        // Range filters and text filters always benefit from SQL push-down
        if (rangeFilters.isNotEmpty() || textFilters.isNotEmpty()) return true
        // Large exclusion sets benefit from SQL NOT EXISTS
        if (exclusionFilters.any { it.excludedValues.size > 100 }) return true
        // Ordering with limit is efficient in SQL
        if (orderBy.isNotEmpty() && limit < Int.MAX_VALUE) return true
        // Large subject/predicate lists are cheaper in SQL IN clauses
        if ((subjects?.size ?: 0) > 100 || (predicates?.size ?: 0) > 100) return true
        return false
    }

    companion object {
        val EMPTY = FilterDescriptor()
        const val CIRCUIT_BREAKER_DEPTH = 5
        const val CIRCUIT_BREAKER_SET_SIZE = 2000
    }
}
