package org.megras.graphstore.deferred

import org.megras.data.graph.*
import org.megras.graphstore.*
import org.megras.graphstore.db.AbstractDbStore
import org.megras.id.SemanticId

class DeferredQuadSet internal constructor(
    private val source: QuadSet,
    internal val descriptor: FilterDescriptor = FilterDescriptor.EMPTY
) : MutableQuadSet {

    // --- MutableSet<Quad> delegation (mutations pass through to source) ---

    private fun mutableSource(): MutableSet<Quad> {
        return source as? MutableSet<Quad>
            ?: throw UnsupportedOperationException("DeferredQuadSet source is not mutable")
    }

    override fun add(element: Quad): Boolean = mutableSource().add(element)
    override fun addAll(elements: Collection<Quad>): Boolean = mutableSource().addAll(elements)
    override fun remove(element: Quad): Boolean = mutableSource().remove(element)
    override fun removeAll(elements: Collection<Quad>): Boolean = mutableSource().removeAll(elements)
    override fun retainAll(elements: Collection<Quad>): Boolean = mutableSource().retainAll(elements)
    override fun clear() = mutableSource().clear()

    // Composable filter methods
    override fun filter(
        subjects: Collection<QuadValue>?,
        predicates: Collection<QuadValue>?,
        objects: Collection<QuadValue>?
    ): QuadSet {
        if (subjects == null && predicates == null && objects == null) return this
        if (descriptor.depth >= FilterDescriptor.CIRCUIT_BREAKER_DEPTH) {
            return materialize().filter(subjects, predicates, objects)
        }
        val sSize = subjects?.size ?: 0
        val pSize = predicates?.size ?: 0
        val oSize = objects?.size ?: 0
        if (sSize > FilterDescriptor.CIRCUIT_BREAKER_SET_SIZE ||
            pSize > FilterDescriptor.CIRCUIT_BREAKER_SET_SIZE ||
            oSize > FilterDescriptor.CIRCUIT_BREAKER_SET_SIZE) {
            return materialize().filter(subjects, predicates, objects)
        }
        val merged = descriptor.mergeFilter(subjects, predicates, objects)
        if (merged.isImpossible) return BasicQuadSet()
        return DeferredQuadSet(source, merged)
    }

    override fun filterSubject(subject: QuadValue): QuadSet = filter(setOf(subject), null, null)
    override fun filterPredicate(predicate: QuadValue): QuadSet = filter(null, setOf(predicate), null)
    override fun filterObject(`object`: QuadValue): QuadSet = filter(null, null, setOf(`object`))

    override fun filterRange(predicate: QuadValue, min: Double?, max: Double?): QuadSet {
        if (descriptor.depth >= FilterDescriptor.CIRCUIT_BREAKER_DEPTH) {
            return materialize().filterRange(predicate, min, max)
        }
        return DeferredQuadSet(source, descriptor.withRangeFilter(predicate, min, max))
    }

    override fun filterNotIn(predicate: QuadValue, excludedValues: Collection<QuadValue>): QuadSet {
        if (descriptor.depth >= FilterDescriptor.CIRCUIT_BREAKER_DEPTH) {
            return materialize().filterNotIn(predicate, excludedValues)
        }
        // filterNotIn is scoped to the predicate (per QuadSet semantics):
        // first restrict to quads with this predicate, then exclude specific objects.
        var merged = descriptor.mergeFilter(null, setOf(predicate), null)
        merged = merged.withExclusionFilter(predicate, excludedValues)
        return DeferredQuadSet(source, merged)
    }

    override fun orderedFilter(
        subjects: Collection<QuadValue>?,
        predicates: Collection<QuadValue>?,
        objects: Collection<QuadValue>?,
        orderBy: List<OrderSpec>,
        limit: Int,
        offset: Int
    ): QuadSet {
        var merged = descriptor.mergeFilter(subjects, predicates, objects)
        merged = merged.withOrdering(orderBy, limit, offset)
        if (!merged.isComposable) {
            return materialize().orderedFilter(subjects, predicates, objects, orderBy, limit, offset)
        }
        return DeferredQuadSet(source, merged)
    }

    override fun textFilter(predicate: QuadValue, objectFilterText: String): QuadSet {
        if (descriptor.depth >= FilterDescriptor.CIRCUIT_BREAKER_DEPTH) {
            return materialize().textFilter(predicate, objectFilterText)
        }
        return DeferredQuadSet(source, descriptor.withTextFilter(predicate, objectFilterText))
    }

    // Materialization-triggering methods
    override fun nearestNeighbor(
        predicate: QuadValue, `object`: VectorValue,
        count: Int, distance: Distance, invert: Boolean
    ): QuadSet = materialize().nearestNeighbor(predicate, `object`, count, distance, invert)

    override fun plus(other: QuadSet): QuadSet = materialize() + other
    override fun toMutable(): MutableQuadSet = materialize().toMutable()
    override fun toSet(): Set<Quad> = materialize().toSet()
    override fun getId(id: SemanticId): Quad? = materialize().getId(id)

    override fun exists(subject: QuadValue, predicate: QuadValue): Boolean {
        if (source is AbstractDbStore) {
            return source.exists(subject, predicate)
        }
        return materialize().exists(subject, predicate)
    }

    override fun distinctObjects(predicate: QuadValue): Set<QuadValue> =
        materialize().distinctObjects(predicate)

    override fun distinctSubjects(predicate: QuadValue): Set<QuadValue> =
        materialize().distinctSubjects(predicate)

    override val size: Int
        get() = if (descriptor.isTrivial) source.size else materialize().size

    override fun isEmpty(): Boolean {
        if (descriptor.isImpossible) return true
        if (descriptor.isTrivial) return source.isEmpty()
        return materialize().isEmpty()
    }

    override fun contains(element: Quad): Boolean = materialize().contains(element)
    override fun containsAll(elements: Collection<Quad>): Boolean = elements.all { contains(it) }
    override fun iterator(): MutableIterator<Quad> = materialize().toList().toMutableList().iterator()

    // Materialization
    fun materialize(): QuadSet {
        if (descriptor.isImpossible) return BasicQuadSet()
        if (descriptor.isTrivial) return source

        // Resolve implicit filters into subject constraints before dispatching
        val effectiveDescriptor = resolveImplicitFilters()
        if (effectiveDescriptor.isImpossible) return BasicQuadSet()

        return if (source is AbstractDbStore) {
            source.materializeFilter(effectiveDescriptor)
        } else {
            materializeViaIterator(effectiveDescriptor)
        }
    }

    /**
     * Evaluate [FilterDescriptor.implicitFilters] by querying the source
     * for each implicit predicate and collecting subjects that satisfy the
     * relation with the reference subject. The matching subjects are intersected
     * with any existing subject constraints and baked into the returned descriptor
     * (implicit filters removed, subjects updated).
     *
     * Returns an impossible descriptor when no subjects satisfy an implicit filter.
     */
    private fun resolveImplicitFilters(): FilterDescriptor {
        if (descriptor.implicitFilters.isEmpty()) return descriptor

        var subjectOverride = descriptor.subjects
        for (imp in descriptor.implicitFilters) {
            val impSubjects = source
                .filterPredicate(imp.predicate)
                .asSequence()
                .filter { it.`object` == imp.referenceSubject }
                .map { it.subject }
                .toSet()
            if (impSubjects.isEmpty()) {
                return FilterDescriptor.EMPTY.copy(subjects = emptySet())
            }
            subjectOverride = if (subjectOverride == null) impSubjects
                             else subjectOverride.intersect(impSubjects).toHashSet()
            if (subjectOverride.isEmpty()) {
                return FilterDescriptor.EMPTY.copy(subjects = emptySet())
            }
        }
        return descriptor.copy(
            implicitFilters = emptyList(),
            subjects = subjectOverride,
        )
    }

    private fun materializeViaIterator(
        descriptor: FilterDescriptor = this.descriptor
    ): QuadSet {
        val subjectSet = descriptor.subjects?.toHashSet()
        val predicateSet = descriptor.predicates?.toHashSet()
        val objectSet = descriptor.objects?.toHashSet()

        val rangeChecks: List<(Quad) -> Boolean> = descriptor.rangeFilters.map { range ->
            val pred = range.predicate
            val min = range.min
            val max = range.max
            fun(quad: Quad): Boolean {
                return quad.predicate == pred && when (val obj = quad.`object`) {
                    is DoubleValue -> (min == null || obj.value >= min) && (max == null || obj.value <= max)
                    is LongValue -> (min == null || obj.value.toDouble() >= min) && (max == null || obj.value.toDouble() <= max)
                    else -> false
                }
            }
        }

        val textChecks: List<(Quad) -> Boolean> = descriptor.textFilters.map { tf ->
            fun(quad: Quad): Boolean {
                return quad.predicate == tf.predicate
                    && quad.`object` is StringValue
                    && quad.`object`.value.contains(tf.searchText, ignoreCase = true)
            }
        }

        val exclusionChecks: List<(Quad) -> Boolean> = descriptor.exclusionFilters.map { ef ->
            val excludedSet = ef.excludedValues.toHashSet()
            fun(quad: Quad): Boolean {
                // Exclude only quads matching the predicate AND having the excluded object value.
                // Pass through all other quads (different predicate, or non-excluded object).
                return quad.predicate != ef.predicate || quad.`object` !in excludedSet
            }
        }

        val result = mutableListOf<Quad>()
        for (quad in source) {
            if ((subjectSet?.contains(quad.subject) != false) &&
                (predicateSet?.contains(quad.predicate) != false) &&
                (objectSet?.contains(quad.`object`) != false) &&
                rangeChecks.all { it(quad) } &&
                textChecks.all { it(quad) } &&
                exclusionChecks.all { it(quad) }) {
                result.add(quad)
            }
        }

        // Apply ordering
        val sorted = if (descriptor.orderBy.isNotEmpty()) {
            result.sortedWith(Comparator { a, b ->
                var cmp = 0
                for (spec in descriptor.orderBy) {
                    val keyA = when (spec.component) {
                        QuadComponent.SUBJECT -> a.subject.toString()
                        QuadComponent.PREDICATE -> a.predicate.toString()
                        QuadComponent.OBJECT -> a.`object`.toString()
                    }
                    val keyB = when (spec.component) {
                        QuadComponent.SUBJECT -> b.subject.toString()
                        QuadComponent.PREDICATE -> b.predicate.toString()
                        QuadComponent.OBJECT -> b.`object`.toString()
                    }
                    cmp = if (spec.ascending) keyA.compareTo(keyB) else keyB.compareTo(keyA)
                    if (cmp != 0) return@Comparator cmp
                }
                cmp
            })
        } else {
            result
        }

        val paged = sorted.drop(descriptor.offset).take(descriptor.limit)
        return BasicQuadSet(paged.toSet())
    }

    companion object {
        fun from(source: QuadSet): DeferredQuadSet =
            DeferredQuadSet(source, FilterDescriptor.EMPTY)
    }
}
