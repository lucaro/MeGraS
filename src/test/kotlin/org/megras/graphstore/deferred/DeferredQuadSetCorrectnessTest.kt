package org.megras.graphstore.deferred

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.megras.data.graph.*
import org.megras.graphstore.*
import org.megras.graphstore.db.PostgresStore
import org.megras.id.id
import org.megras.graphstore.db.dict.PostgresDictionary
import java.net.InetSocketAddress
import java.net.Socket
import java.sql.DriverManager

/**
 * Comprehensive correctness suite for [DeferredQuadSet] and [FilterDescriptor].
 *
 * Supplements [DeferredQuadSetTest] with edge-case, circuit-breaker, mutation,
 * ordering, combined-filter, and text-filter coverage.
 *
 * Self-skips Postgres tests (sections F/G) unless localhost:5433 is reachable.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DeferredQuadSetCorrectnessTest {

    // ─── Corpus ───
    private val s1 = QuadValue.of("http://ex/s1")
    private val s2 = QuadValue.of("http://ex/s2")
    private val s3 = QuadValue.of("http://ex/s3") // not in corpus
    private val p1 = QuadValue.of("http://ex/p1")
    private val p2 = QuadValue.of("http://ex/p2")
    private val pt = QuadValue.of("http://ex/pt")  // text predicate
    private val ou1 = QuadValue.of("http://ex/o1")
    private val ou2 = QuadValue.of("http://ex/o2")
    private val ou3 = QuadValue.of("http://ex/o3")
    private val ou4 = QuadValue.of("http://ex/o4")

    private val corpus = listOf(
        Quad(s1, p1, ou1),
        Quad(s1, p2, ou2),
        Quad(s2, p1, ou3),
        Quad(s2, p2, DoubleValue(1.5)),
        Quad(s2, p1, DoubleValue(3.0)),  // extra p1 for range tests
        Quad(s1, pt, StringValue("The quick brown fox")),
        Quad(s2, pt, StringValue("The lazy brown dog")),
    )

    private fun norm(qs: QuadSet): Set<Triple<QuadValue, QuadValue, QuadValue>> =
        qs.map { Triple(it.subject, it.predicate, it.`object`) }.toSet()

    private fun build(): BasicQuadSet = BasicQuadSet(corpus.toSet())

    private fun ds(): DeferredQuadSet = DeferredQuadSet.from(build())

    // ─── Postgres ───
    private var pgStore: PostgresStore? = null

    private fun pgAvailable(): Boolean = try {
        Socket().use { it.connect(InetSocketAddress("localhost", 5433), 200); true }
    } catch (_: Exception) {
        false
    }

    private fun resetSchema() {
        DriverManager.getConnection(
            "jdbc:postgresql://localhost:5433/megras_test", "megras", "megras"
        ).use { c ->
            c.createStatement().use { it.execute("DROP SCHEMA IF EXISTS megras CASCADE;") }
        }
    }

    @BeforeAll
    fun setupPg() {
        if (!pgAvailable()) return
        resetSchema()
        val dict = PostgresDictionary("localhost:5433/megras_test", "megras", "megras")
        val store = PostgresStore(dict, "localhost:5433/megras_test", "megras", "megras")
        dict.setup()
        store.setup()
        for (q in corpus) store.add(q)
        pgStore = store
    }

    private fun pg(): PostgresStore {
        assumeTrue(pgStore != null, "Postgres not available on localhost:5433")
        return pgStore!!
    }

    private fun pgDs(): DeferredQuadSet = DeferredQuadSet.from(pg())

    // ═══════════════════════════════════════════════════════════════════════
    //  A. FilterDescriptor edge cases
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    fun `descriptor - non-intersecting subjects make impossible`() {
        val d = FilterDescriptor.EMPTY
            .mergeFilter(setOf(s1), null, null)
            .mergeFilter(setOf(s2), null, null)
        assertTrue(d.isImpossible) // s1 ∩ s2 = ∅
    }

    @Test
    fun `descriptor - mergeFilter all nulls is no-op`() {
        val d = FilterDescriptor.EMPTY.mergeFilter(null, null, null)
        assertEquals(FilterDescriptor.EMPTY, d)
    }

    @Test
    fun `descriptor - isImpossible on empty predicates`() {
        val d = FilterDescriptor.EMPTY.mergeFilter(null, emptySet(), null)
        assertTrue(d.isImpossible)
    }

    @Test
    fun `descriptor - isImpossible on empty objects`() {
        val d = FilterDescriptor.EMPTY.mergeFilter(null, null, emptySet())
        assertTrue(d.isImpossible)
    }

    @Test
    fun `descriptor - rangeFilter with null min`() {
        val d = FilterDescriptor.EMPTY.withRangeFilter(p1, null, 10.0)
        assertEquals(1, d.rangeFilters.size)
        assertNull(d.rangeFilters[0].min)
        assertEquals(10.0, d.rangeFilters[0].max)
    }

    @Test
    fun `descriptor - rangeFilter with null max`() {
        val d = FilterDescriptor.EMPTY.withRangeFilter(p1, 5.0, null)
        assertEquals(5.0, d.rangeFilters[0].min)
        assertNull(d.rangeFilters[0].max)
    }

    @Test
    fun `descriptor - rangeFilter with both null is vacuous`() {
        val d = FilterDescriptor.EMPTY.withRangeFilter(p1, null, null)
        assertNotNull(d.rangeFilters[0])
    }

    @Test
    fun `descriptor - withTextFilter stores predicate and text`() {
        val d = FilterDescriptor.EMPTY.withTextFilter(pt, "fox")
        assertEquals(1, d.textFilters.size)
        assertEquals(pt, d.textFilters[0].predicate)
        assertEquals("fox", d.textFilters[0].searchText)
    }

    @Test
    fun `descriptor - withExclusionFilter stores predicate and values`() {
        val d = FilterDescriptor.EMPTY.withExclusionFilter(p1, setOf(ou1, ou2))
        assertEquals(1, d.exclusionFilters.size)
        assertEquals(p1, d.exclusionFilters[0].predicate)
        assertEquals(setOf(ou1, ou2), d.exclusionFilters[0].excludedValues.toSet())
    }

    @Test
    fun `descriptor - withOrdering stores offset`() {
        val d = FilterDescriptor.EMPTY
            .withOrdering(listOf(OrderSpec(QuadComponent.SUBJECT, true)), limit = 10, offset = 5)
        assertEquals(5, d.offset)
        assertEquals(10, d.limit)
    }

    @Test
    fun `descriptor - isTrivial only when all fields default`() {
        val base = FilterDescriptor.EMPTY
        assertTrue(base.isTrivial)
        assertFalse(base.withRangeFilter(p1, 0.0, 1.0).isTrivial)
        assertFalse(base.withTextFilter(pt, "x").isTrivial)
        assertFalse(base.withExclusionFilter(p1, setOf(ou1)).isTrivial)
        assertFalse(base.withOrdering(listOf(OrderSpec(QuadComponent.OBJECT, true)), 10).isTrivial)
        assertFalse(base.mergeFilter(setOf(s1), null, null).isTrivial)
    }

    @Test
    fun `descriptor - isComposable true below circuit breaker`() {
        var d = FilterDescriptor.EMPTY
        repeat(FilterDescriptor.CIRCUIT_BREAKER_DEPTH - 1) {
            d = d.mergeFilter(setOf(s1), null, null)
        }
        assertTrue(d.isComposable)
    }

    @Test
    fun `descriptor - isComposable false at circuit breaker`() {
        var d = FilterDescriptor.EMPTY
        repeat(FilterDescriptor.CIRCUIT_BREAKER_DEPTH) {
            d = d.mergeFilter(setOf(s1), null, null)
        }
        assertFalse(d.isComposable)
    }

    @Test
    fun `descriptor - immutability of data class`() {
        val original = FilterDescriptor.EMPTY.mergeFilter(setOf(s1), null, null)
        val derived = original.mergeFilter(null, setOf(p1), null)
        // Original unchanged
        assertNull(original.predicates)
        // Derived has both
        assertEquals(setOf(s1), derived.subjects)
        assertEquals(setOf(p1), derived.predicates)
    }

    @Test
    fun `descriptor - depth starts at zero`() {
        assertEquals(0, FilterDescriptor.EMPTY.depth)
    }

    @Test
    fun `descriptor - multiple range filters accumulate`() {
        val d = FilterDescriptor.EMPTY
            .withRangeFilter(p1, 0.0, 10.0)
            .withRangeFilter(p2, 5.0, 20.0)
            .withRangeFilter(pt, null, null)
        assertEquals(3, d.rangeFilters.size)
    }

    @Test
    fun `descriptor - multiple text filters accumulate`() {
        val d = FilterDescriptor.EMPTY
            .withTextFilter(pt, "fox")
            .withTextFilter(pt, "dog")
        assertEquals(2, d.textFilters.size)
    }

    @Test
    fun `descriptor - multiple exclusion filters accumulate`() {
        val d = FilterDescriptor.EMPTY
            .withExclusionFilter(p1, setOf(ou1))
            .withExclusionFilter(p2, setOf(ou2))
        assertEquals(2, d.exclusionFilters.size)
    }

    @Test
    fun `descriptor - intersectSets reduces`() {
        val d = FilterDescriptor.EMPTY
            .mergeFilter(setOf(s1, s2), null, null)
            .mergeFilter(setOf(s1), null, null)
        assertEquals(setOf(s1), d.subjects)
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  B. Filter composition correctness
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    fun `filter - textFilter matches substrings`() {
        val result = norm(ds().textFilter(pt, "fox"))
        assertEquals(1, result.size)
        assertEquals(s1, result.first().first)
    }

    @Test
    fun `filter - textFilter is case insensitive`() {
        val result = norm(ds().textFilter(pt, "FOX"))
        // textFilter uses ignoreCase=true, so "FOX" matches "The quick brown fox"
        assertEquals(1, result.size)
        assertEquals(s1, result.first().first)
    }

    @Test
    fun `filter - textFilter no match returns empty`() {
        val result = norm(ds().textFilter(pt, "elephant"))
        assertTrue(result.isEmpty())
    }

    @Test
    fun `filter - textFilter on non-existent predicate returns empty`() {
        val result = norm(ds().textFilter(p1, "fox"))
        assertTrue(result.isEmpty())
    }

    @Test
    fun `filter - textFilter on non-string object is safe`() {
        // p2 has DoubleValue(1.5) for s2 - textFilter should skip non-string objects
        val result = norm(ds().textFilter(p2, "1"))
        assertTrue(result.isEmpty())
    }

    @Test
    fun `filter - filterRange null min means no lower bound`() {
        val result = norm(ds().filterRange(p2, null, 2.0))
        assertEquals(1, result.size)
        assertEquals(DoubleValue(1.5), result.first().third)
    }

    @Test
    fun `filter - filterRange null max means no upper bound`() {
        val result = norm(ds().filterRange(p2, 1.0, null))
        assertEquals(1, result.size)
        assertEquals(DoubleValue(1.5), result.first().third)
    }

    @Test
    fun `filter - filterRange both null returns all numeric predicate quads`() {
        // filterRange only accepts numeric (Double/Long) objects; URIValue objects are excluded
        // by design (see QuadSet.filterRange). With null bounds, all numeric quads for p2 pass.
        val result = norm(ds().filterRange(p2, null, null))
        // p2 quads: (s1,p2,ou2) — URIValue, excluded; (s2,p2,DoubleValue(1.5)) — numeric, included
        val p2Numeric = corpus.filter { it.predicate == p2 && (it.`object` is DoubleValue || it.`object` is LongValue) }
        assertEquals(p2Numeric.size, result.size)
    }

    @Test
    fun `filter - filterNotIn with empty excluded set returns all predicate quads`() {
        val result = norm(ds().filterNotIn(p1, emptySet()))
        val p1Quads = corpus.filter { it.predicate == p1 }.map {
            Triple(it.subject, it.predicate, it.`object`)
        }.toSet()
        assertEquals(p1Quads, result)
    }

    @Test
    fun `filter - filterNotIn with multiple excluded values`() {
        val result = norm(ds().filterNotIn(p1, setOf(ou1, ou3)))
        // p1 quads: (s1,p1,ou1), (s2,p1,ou3), (s2,p1,DoubleValue(3.0))
        // After excluding ou1 and ou3: only (s2,p1,DoubleValue(3.0))
        assertEquals(1, result.size)
        assertEquals(DoubleValue(3.0), result.first().third)
    }

    @Test
    fun `filter - filterNotIn excluding all values returns empty`() {
        val allP1Objects = corpus.filter { it.predicate == p1 }.map { it.`object` }.toSet()
        val result = norm(ds().filterNotIn(p1, allP1Objects))
        assertTrue(result.isEmpty())
    }

    @Test
    fun `filter - filterNotIn on non-existent predicate returns empty`() {
        val result = norm(ds().filterNotIn(s3, setOf(ou1)))
        assertTrue(result.isEmpty())
    }

    @Test
    fun `filter - combined range plus text filter`() {
        // p2 has only DoubleValue(1.5), text filter "1" matches "1.5" toString
        // But textFilter requires StringValue, so combined should be empty
        val result = norm(ds().filterRange(p2, 0.0, 2.0).textFilter(pt, "fox"))
        // filterRange only returns p2 quads, textFilter only returns pt quads
        // Intersection is empty
        assertTrue(result.isEmpty())
    }

    @Test
    fun `filter - combined subject filter plus text filter`() {
        val result = norm(ds().filterSubject(s1).textFilter(pt, "fox"))
        assertEquals(1, result.size)
        val (sub, pred, obj) = result.first()
        assertEquals(s1, sub)
        assertEquals(pt, pred)
    }

    @Test
    fun `filter - combined predicate plus exclusion plus range`() {
        // filter p1, exclude ou1, range [0, 10]
        // p1 quads: (s1,p1,ou1), (s2,p1,ou3), (s2,p1,DoubleValue(3.0))
        // After exclusion of ou1: (s2,p1,ou3), (s2,p1,DoubleValue(3.0))
        // After range [0,10]: (s2,p1,DoubleValue(3.0)) only (ou3 is not numeric)
        val result = norm(ds().filterPredicate(p1).filterNotIn(p1, setOf(ou1)).filterRange(p1, 0.0, 10.0))
        assertEquals(1, result.size)
        assertEquals(DoubleValue(3.0), result.first().third)
    }

    @Test
    fun `filter - triple intersection subjects AND predicates AND objects`() {
        // Only (s1, p1, ou1) should match all three
        val result = norm(ds().filter(setOf(s1), setOf(p1), setOf(ou1)))
        assertEquals(setOf(Triple(s1, p1, ou1)), result)
    }

    @Test
    fun `filter - filterSubject for non-existent subject`() {
        val result = norm(ds().filterSubject(s3))
        assertTrue(result.isEmpty())
    }

    @Test
    fun `filter - filterObject for non-existent object`() {
        val result = norm(ds().filterObject(ou4))
        assertTrue(result.isEmpty())
    }

    @Test
    fun `filter - no-op filter with all nulls`() {
        val qs = ds()
        val result = qs.filter(null, null, null)
        assertSame(qs, result)
    }

    @Test
    fun `filter - chained predicate intersection narrows`() {
        val result = norm(ds().filterPredicate(p1).filterPredicate(p2))
        assertTrue(result.isEmpty()) // no quad has both p1 AND p2
    }

    @Test
    fun `filter - chained subject intersection narrows`() {
        val result = norm(ds().filterSubject(s1).filterSubject(s2))
        assertTrue(result.isEmpty())
    }

    @Test
    fun `filter - multiple range filters on different predicates`() {
        // p2 in range [0,2] AND p1 in range [2,4]
        val result = norm(ds().filterRange(p2, 0.0, 2.0).filterRange(p1, 2.0, 4.0))
        // p2 range gives (s2,p2,1.5). p1 range gives (s2,p1,3.0)
        // But these are DIFFERENT quads — intersection requires same quad to match all
        // Since no single quad has both p1 and p2, result is empty
        assertTrue(result.isEmpty())
    }

    @Test
    fun `filter - multiple exclusion filters on different predicates`() {
        val result = norm(ds().filterNotIn(p1, setOf(ou1)).filterNotIn(p2, setOf(ou2)))
        // p1 minus ou1: (s2,p1,ou3), (s2,p1,DoubleValue(3.0))
        // p2 minus ou2: (s2,p2,DoubleValue(1.5))
        // Combined: predicate must be in BOTH p1 AND p2 — impossible
        assertTrue(result.isEmpty())
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  C. Circuit breaker behavior
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    fun `circuit breaker - depth threshold triggers materialization`() {
        var qs: QuadSet = ds()
        // Chain exactly CIRCUIT_BREAKER_DEPTH filters
        repeat(FilterDescriptor.CIRCUIT_BREAKER_DEPTH) {
            qs = qs.filterPredicate(p1)
        }
        // At this point the last filter should have triggered materialization
        // because the previous one was already at depth >= breaker.
        // Result should still be correct: all p1 quads
        val expected = norm(ds().filterPredicate(p1))
        assertEquals(expected, norm(qs))
    }

    @Test
    fun `circuit breaker - set size threshold triggers materialization`() {
        // Create a huge set of subjects to trigger size threshold circuit breaker
        val hugeSet = (1..FilterDescriptor.CIRCUIT_BREAKER_SET_SIZE + 1).map {
            QuadValue.of("http://ex/s_$it")
        }.toSet()
        val qs = ds().filter(hugeSet, null, null)
        // None of the huge subjects exist in corpus, so result should be empty
        // (circuit breaker fires, materializes, then filters — correct but empty result)
        assertTrue(qs.isEmpty())
    }

    @Test
    fun `circuit breaker - filterRange at depth triggers materialize`() {
        var qs: QuadSet = ds()
        repeat(FilterDescriptor.CIRCUIT_BREAKER_DEPTH) {
            qs = qs.filterRange(p2, 0.0, 10.0)
        }
        val expected = norm(ds().filterRange(p2, 0.0, 10.0))
        assertEquals(expected, norm(qs))
    }

    @Test
    fun `circuit breaker - filterNotIn at depth triggers materialize`() {
        var qs: QuadSet = ds()
        repeat(FilterDescriptor.CIRCUIT_BREAKER_DEPTH) {
            qs = qs.filterNotIn(p1, setOf(ou1))
        }
        val expected = norm(ds().filterNotIn(p1, setOf(ou1)))
        assertEquals(expected, norm(qs))
    }

    @Test
    fun `circuit breaker - textFilter at depth triggers materialize`() {
        var qs: QuadSet = ds()
        repeat(FilterDescriptor.CIRCUIT_BREAKER_DEPTH) {
            qs = qs.textFilter(pt, "fox")
        }
        val expected = norm(ds().textFilter(pt, "fox"))
        assertEquals(expected, norm(qs))
    }

    @Test
    fun `circuit breaker - orderedFilter at depth triggers materialize`() {
        var qs: QuadSet = ds()
        repeat(FilterDescriptor.CIRCUIT_BREAKER_DEPTH) {
            qs = qs.orderedFilter(
                null, null, null,
                listOf(OrderSpec(QuadComponent.OBJECT, true)),
                limit = Int.MAX_VALUE
            )
        }
        val expected = ds().orderedFilter(
            null, null, null,
            listOf(OrderSpec(QuadComponent.OBJECT, true)),
            limit = Int.MAX_VALUE
        ).toList()
        assertEquals(expected, qs.toList())
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  D. Ordering correctness
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    fun `ordering - descending sort`() {
        val qs = ds().filterPredicate(p1)
        val result = qs.orderedFilter(
            null, null, null,
            listOf(OrderSpec(QuadComponent.OBJECT, false)),
            limit = Int.MAX_VALUE
        ).toList()
        val objects = result.map { it.`object`.toString() }
        assertEquals(objects.sortedDescending(), objects)
    }

    @Test
    fun `ordering - sort by subject`() {
        val qs = ds().filterPredicate(p1)
        val result = qs.orderedFilter(
            null, null, null,
            listOf(OrderSpec(QuadComponent.SUBJECT, true)),
            limit = Int.MAX_VALUE
        ).toList()
        val subjects = result.map { it.subject.toString() }
        assertEquals(subjects.sorted(), subjects)
    }

    @Test
    fun `ordering - sort by predicate`() {
        val result = ds().orderedFilter(
            null, null, null,
            listOf(OrderSpec(QuadComponent.PREDICATE, true)),
            limit = Int.MAX_VALUE
        ).toList()
        val predicates = result.map { it.predicate.toString() }
        assertEquals(predicates.sorted(), predicates)
    }

    @Test
    fun `ordering - compound sort key subject then object`() {
        val result = ds().orderedFilter(
            null, null, null,
            listOf(
                OrderSpec(QuadComponent.SUBJECT, true),
                OrderSpec(QuadComponent.OBJECT, true)
            ),
            limit = Int.MAX_VALUE
        ).toList()
        // Verify: first sorted by subject, then by object within same subject
        val pairs = result.map { it.subject.toString() to it.`object`.toString() }
        val sortedPairs = pairs.sortedWith(compareBy<Pair<String, String>> { it.first }.thenBy { it.second })
        assertEquals(sortedPairs, pairs)
    }

    @Test
    fun `ordering - compound sort key predicate ascending then object descending`() {
        val result = ds().orderedFilter(
            null, null, null,
            listOf(
                OrderSpec(QuadComponent.PREDICATE, true),
                OrderSpec(QuadComponent.OBJECT, false)
            ),
            limit = Int.MAX_VALUE
        ).toList()
        // Group by predicate, verify within each group objects are descending
        val grouped = result.groupBy { it.predicate.toString() }
        for ((_, quads) in grouped) {
            val objects = quads.map { it.`object`.toString() }
            assertEquals(objects.sortedDescending(), objects)
        }
    }

    @Test
    fun `ordering - limit 0 returns empty`() {
        val result = ds().orderedFilter(
            null, null, null,
            listOf(OrderSpec(QuadComponent.OBJECT, true)),
            limit = 0
        )
        assertTrue(result.isEmpty())
    }

    @Test
    fun `ordering - offset beyond total returns empty`() {
        val total = ds().size
        val result = ds().orderedFilter(
            null, null, null,
            listOf(OrderSpec(QuadComponent.OBJECT, true)),
            limit = Int.MAX_VALUE,
            offset = total + 1
        )
        assertTrue(result.isEmpty())
    }

    @Test
    fun `ordering - offset equal to total returns empty`() {
        val total = ds().size
        val result = ds().orderedFilter(
            null, null, null,
            listOf(OrderSpec(QuadComponent.OBJECT, true)),
            limit = Int.MAX_VALUE,
            offset = total
        )
        assertTrue(result.isEmpty())
    }

    @Test
    fun `ordering - limit larger than result set returns all`() {
        val total = ds().size
        val result = ds().orderedFilter(
            null, null, null,
            listOf(OrderSpec(QuadComponent.OBJECT, true)),
            limit = total + 100
        )
        assertEquals(total, result.size)
    }

    @Test
    fun `ordering - combined filter then sort then paginate`() {
        val result = ds().filterPredicate(p1)
            .orderedFilter(
                null, null, null,
                listOf(OrderSpec(QuadComponent.OBJECT, true)),
                limit = 1,
                offset = 1
            ).toList()
        assertEquals(1, result.size)
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  E. Mutation pass-through
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    fun `mutation - add increments size`() {
        val bqs = BasicMutableQuadSet(corpus.toMutableList())
        val ds = DeferredQuadSet.from(bqs)
        val initialSize = ds.size
        val newQuad = Quad(QuadValue.of("http://ex/newS"), p1, QuadValue.of("http://ex/newO"))
        ds.add(newQuad)
        assertEquals(initialSize + 1, ds.size)
        assertTrue(ds.exists(QuadValue.of("http://ex/newS"), p1))
    }

    @Test
    fun `mutation - remove decrements size`() {
        val bqs = BasicMutableQuadSet(corpus.toMutableList())
        val ds = DeferredQuadSet.from(bqs)
        val initialSize = ds.size
        ds.remove(corpus[0])
        assertEquals(initialSize - 1, ds.size)
    }

    @Test
    fun `mutation - addAll adds multiple`() {
        val bqs = BasicMutableQuadSet(mutableSetOf())
        val ds = DeferredQuadSet.from(bqs)
        assertTrue(ds.isEmpty())
        ds.addAll(corpus)
        assertEquals(corpus.size, ds.size)
    }

    @Test
    fun `mutation - clear empties source`() {
        val bqs = BasicMutableQuadSet(corpus.toMutableList())
        val ds = DeferredQuadSet.from(bqs)
        assertFalse(ds.isEmpty())
        ds.clear()
        assertTrue(ds.isEmpty())
        assertEquals(0, ds.size)
    }

    @Test
    fun `mutation - removeAll by matching quads`() {
        val bqs = BasicMutableQuadSet(corpus.toMutableList())
        val ds = DeferredQuadSet.from(bqs)
        val initialSize = ds.size
        // Remove all p1 quads
        val p1Quads = corpus.filter { it.predicate == p1 }
        ds.removeAll(p1Quads)
        assertEquals(initialSize - p1Quads.size, ds.size)
        assertFalse(ds.exists(s1, p1))
    }

    @Test
    fun `mutation - retainAll keeps only matching`() {
        val bqs = BasicMutableQuadSet(corpus.toMutableList())
        val ds = DeferredQuadSet.from(bqs)
        // Retain only p1 quads
        val p1Quads = corpus.filter { it.predicate == p1 }
        ds.retainAll(p1Quads)
        assertEquals(p1Quads.size, ds.size)
        // Verify all remaining have p1
        for (q in ds) {
            assertEquals(p1, q.predicate)
        }
    }

    @Test
    fun `mutation - add through deferred is visible through another deferred`() {
        val bqs = BasicMutableQuadSet(mutableSetOf())
        val ds1 = DeferredQuadSet.from(bqs)
        val ds2 = DeferredQuadSet.from(bqs)
        val q = Quad(s1, p1, ou1)
        ds1.add(q)
        assertTrue(ds2.exists(s1, p1))
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  F. Materialization behavior
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    fun `materialize - trivial returns source`() {
        val bqs = build()
        val ds = DeferredQuadSet.from(bqs)
        assertSame(bqs, ds.materialize())
    }

    @Test
    fun `materialize - impossible returns empty`() {
        // filter with empty set produces BasicQuadSet directly, not a DeferredQuadSet
        val qs = ds().filter(emptySet(), null, null)
        assertTrue(qs.isEmpty())
    }

    @Test
    fun `materialize - non-trivial returns BasicQuadSet for in-memory`() {
        val deferred = DeferredQuadSet.from(build()).filterSubject(s1) as DeferredQuadSet
        val result = deferred.materialize()
        assertTrue(result is BasicQuadSet)
    }

    @Test
    fun `materialize - result contains correct quads`() {
        val deferred = DeferredQuadSet.from(build()).filterSubject(s1) as DeferredQuadSet
        val result = deferred.materialize()
        assertEquals(3, result.size)
        assertTrue(result.all { it.subject == s1 })
    }

    @Test
    fun `toSet - returns all quads from materialization`() {
        val ds = ds().filterPredicate(p1)
        val set = ds.toSet()
        assertEquals(corpus.filter { it.predicate == p1 }.size, set.size)
    }

    @Test
    fun `toMutable - returns mutable copy`() {
        val ds = ds().filterSubject(s1)
        val mutable = ds.toMutable()
        val initialSize = mutable.size
        mutable.add(Quad(QuadValue.of("http://ex/newS"), p1, ou1))
        assertEquals(initialSize + 1, mutable.size)
        // Original deferred unchanged
        assertEquals(3, ds.size)
    }

    @Test
    fun `contains - true for existing quad`() {
        assertTrue(ds().contains(corpus[0]))
    }

    @Test
    fun `contains - false for non-existing quad`() {
        val q = Quad(s3, p1, ou1)
        assertFalse(ds().contains(q))
    }

    @Test
    fun `containsAll - true for subset`() {
        assertTrue(ds().containsAll(corpus.take(3).toSet()))
    }

    @Test
    fun `containsAll - false when missing elements`() {
        val extra = Quad(s3, p1, ou1)
        assertFalse(ds().containsAll(corpus.toSet() + extra))
    }

    @Test
    fun `isEmpty - false for non-empty`() {
        assertFalse(ds().isEmpty())
    }

    @Test
    fun `isEmpty - true for impossible`() {
        val ds = ds().filter(emptySet(), null, null)
        assertTrue(ds.isEmpty())
    }

    @Test
    fun `isEmpty - false for empty source`() {
        val ds = DeferredQuadSet.from(BasicQuadSet(emptySet()))
        assertTrue(ds.isEmpty())
    }

    @Test
    fun `size - correct after filter chain`() {
        val size = ds().filterSubject(s1).filterPredicate(p1).size
        assertEquals(1, size) // only (s1, p1, ou1)
    }

    @Test
    fun `iterator - returns all quads`() {
        val ds = ds().filterSubject(s1)
        val iterated = ds.iterator().asSequence().toList()
        assertEquals(3, iterated.size)
        assertTrue(iterated.all { it.subject == s1 })
    }

    @Test
    fun `iterator - works correctly`() {
        val bqs = BasicMutableQuadSet(corpus.toMutableList())
        val ds = DeferredQuadSet.from(bqs)
        val iter = ds.iterator()
        var count = 0
        while (iter.hasNext()) {
            iter.next()
            count++
        }
        assertEquals(corpus.size, count)
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  G. Edge cases with empty/single-element corpus
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    fun `edge - empty corpus all filters return empty`() {
        val ds = DeferredQuadSet.from(BasicQuadSet(emptySet()))
        assertTrue(ds.filterSubject(s1).isEmpty())
        assertTrue(ds.filterPredicate(p1).isEmpty())
        assertTrue(ds.filterObject(ou1).isEmpty())
        assertTrue(ds.filterRange(p2, 0.0, 10.0).isEmpty())
        assertTrue(ds.filterNotIn(p1, setOf(ou1)).isEmpty())
        assertTrue(ds.textFilter(pt, "fox").isEmpty())
    }

    @Test
    fun `edge - single quad corpus filter match`() {
        val q = Quad(s1, p1, ou1)
        val ds = DeferredQuadSet.from(BasicQuadSet(setOf(q)))
        assertEquals(1, ds.filterSubject(s1).size)
        assertEquals(1, ds.filterPredicate(p1).size)
        assertEquals(1, ds.filterObject(ou1).size)
        assertEquals(1, ds.filter(setOf(s1), setOf(p1), setOf(ou1)).size)
    }

    @Test
    fun `edge - single quad corpus filter no match`() {
        val q = Quad(s1, p1, ou1)
        val ds = DeferredQuadSet.from(BasicQuadSet(setOf(q)))
        assertEquals(0, ds.filterSubject(s2).size)
        assertEquals(0, ds.filterPredicate(p2).size)
        assertEquals(0, ds.filterObject(ou2).size)
    }

    @Test
    fun `edge - ordering on empty result set`() {
        val ds = DeferredQuadSet.from(BasicQuadSet(emptySet()))
        val result = ds.orderedFilter(
            null, null, null,
            listOf(OrderSpec(QuadComponent.OBJECT, true)),
            limit = Int.MAX_VALUE
        )
        assertTrue(result.isEmpty())
    }

    @Test
    fun `edge - exists on empty corpus`() {
        val ds = DeferredQuadSet.from(BasicQuadSet(emptySet()))
        assertFalse(ds.exists(s1, p1))
    }

    @Test
    fun `edge - distinctObjects on filtered set`() {
        val ds = ds().filterSubject(s1)
        val distinct = ds.distinctObjects(p1)
        assertEquals(setOf(ou1), distinct)
    }

    @Test
    fun `edge - distinctSubjects on filtered set`() {
        val ds = ds().filterPredicate(p1)
        val distinct = ds.distinctSubjects(p1)
        assertEquals(setOf(s1, s2), distinct)
    }

    @Test
    fun `edge - plus operator combines two deferred sets`() {
        val a = ds().filterSubject(s1)
        val b = ds().filterSubject(s2)
        val combined = a + b
        assertEquals(corpus.size, combined.size)
    }

    @Test
    fun `edge - getId returns null for non-existent`() {
        // Create a SemanticId from a quad that doesn't exist in the corpus
        val fakeQuad = Quad(s3, p1, ou1)
        val fakeId = fakeQuad.id // content hash of a non-existent quad
        assertNull(ds().getId(fakeId))
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  H. PostgresStore integration (requires container)
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    fun `pg - textFilter matches substrings`() {
        val result = norm(pgDs().textFilter(pt, "fox"))
        assertEquals(1, result.size)
        assertEquals(s1, result.first().first)
    }

    @Test
    fun `pg - textFilter no match`() {
        val result = norm(pgDs().textFilter(pt, "elephant"))
        assertTrue(result.isEmpty())
    }

    @Test
    fun `pg - filterNotIn with multiple exclusions`() {
        val result = norm(pgDs().filterNotIn(p1, setOf(ou1, ou3)))
        assertEquals(1, result.size)
        assertEquals(DoubleValue(3.0), result.first().third)
    }

    @Test
    fun `pg - filterRange with null min`() {
        val result = norm(pgDs().filterRange(p2, null, 2.0))
        assertEquals(1, result.size)
        assertEquals(DoubleValue(1.5), result.first().third)
    }

    @Test
    fun `pg - filterRange with null max`() {
        val result = norm(pgDs().filterRange(p2, 1.0, null))
        assertEquals(1, result.size)
        assertEquals(DoubleValue(1.5), result.first().third)
    }

    @Test
    fun `pg - filterSubject then filterObject`() {
        val result = norm(pgDs().filterSubject(s1).filterObject(ou1))
        assertEquals(setOf(Triple(s1, p1, ou1)), result)
    }

    @Test
    fun `pg - orderedFilter descending sort`() {
        val result = pgDs().filterPredicate(p1)
            .orderedFilter(
                null, null, null,
                listOf(OrderSpec(QuadComponent.OBJECT, false)),
                limit = Int.MAX_VALUE
            ).toList()
        val objects = result.map { it.`object`.toString() }
        assertEquals(objects.sortedDescending(), objects)
    }

    @Test
    fun `pg - orderedFilter offset beyond total`() {
        val result = pgDs().orderedFilter(
            null, null, null,
            listOf(OrderSpec(QuadComponent.OBJECT, true)),
            limit = Int.MAX_VALUE,
            offset = corpus.size + 10
        )
        assertTrue(result.isEmpty())
    }

    @Test
    fun `pg - size matches corpus`() {
        assertEquals(corpus.size, pgDs().size)
    }

    @Test
    fun `pg - isEmpty on non-empty`() {
        assertFalse(pgDs().isEmpty())
    }

    @Test
    fun `pg - contains existing quad`() {
        assertTrue(pgDs().contains(corpus[0]))
    }

    @Test
    fun `pg - containsAll for full corpus`() {
        assertTrue(pgDs().containsAll(corpus.toSet()))
    }

    @Test
    fun `pg - exists for existing pair`() {
        assertTrue(pgDs().exists(s1, p1))
    }

    @Test
    fun `pg - exists for non-existing pair`() {
        assertFalse(pgDs().exists(s3, p1))
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  I. Cross-backend parity for new filter types
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    fun `parity - textFilter BQS vs PG`() {
        val bqs = build()
        val bqsResult = norm(DeferredQuadSet.from(bqs).textFilter(pt, "fox"))
        val pgResult = norm(pgDs().textFilter(pt, "fox"))
        assertEquals(bqsResult, pgResult)
    }

    @Test
    fun `parity - filterNotIn multiple exclusions`() {
        val bqsResult = norm(ds().filterNotIn(p1, setOf(ou1, ou3)))
        val pgResult = norm(pgDs().filterNotIn(p1, setOf(ou1, ou3)))
        assertEquals(bqsResult, pgResult)
    }

    @Test
    fun `parity - filterRange null min`() {
        val bqsResult = norm(ds().filterRange(p2, null, 2.0))
        val pgResult = norm(pgDs().filterRange(p2, null, 2.0))
        assertEquals(bqsResult, pgResult)
    }

    @Test
    fun `parity - filterRange null max`() {
        val bqsResult = norm(ds().filterRange(p2, 1.0, null))
        val pgResult = norm(pgDs().filterRange(p2, 1.0, null))
        assertEquals(bqsResult, pgResult)
    }

    @Test
    fun `parity - orderedFilter descending`() {
        val orderSpec = listOf(OrderSpec(QuadComponent.OBJECT, false))
        val bqsResult = ds().filterPredicate(p1)
            .orderedFilter(null, null, null, orderSpec, limit = Int.MAX_VALUE).toList()
        val pgResult = pgDs().filterPredicate(p1)
            .orderedFilter(null, null, null, orderSpec, limit = Int.MAX_VALUE).toList()
        assertEquals(
            bqsResult.map { Triple(it.subject, it.predicate, it.`object`) },
            pgResult.map { Triple(it.subject, it.predicate, it.`object`) }
        )
    }

    @Test
    fun `parity - compound sort key`() {
        val orderSpec = listOf(
            OrderSpec(QuadComponent.PREDICATE, true),
            OrderSpec(QuadComponent.OBJECT, true)
        )
        val bqsResult = ds().orderedFilter(null, null, null, orderSpec, limit = Int.MAX_VALUE).toList()
        val pgResult = pgDs().orderedFilter(null, null, null, orderSpec, limit = Int.MAX_VALUE).toList()
        assertEquals(
            bqsResult.map { Triple(it.subject, it.predicate, it.`object`) },
            pgResult.map { Triple(it.subject, it.predicate, it.`object`) }
        )
    }

    @Test
    fun `parity - complex chain subject + text + order`() {
        val orderSpec = listOf(OrderSpec(QuadComponent.OBJECT, true))
        val bqsResult = ds().filterSubject(s1).textFilter(pt, "quick")
            .orderedFilter(null, null, null, orderSpec, limit = 10).toList()
        val pgResult = pgDs().filterSubject(s1).textFilter(pt, "quick")
            .orderedFilter(null, null, null, orderSpec, limit = 10).toList()
        assertEquals(
            bqsResult.map { Triple(it.subject, it.predicate, it.`object`) },
            pgResult.map { Triple(it.subject, it.predicate, it.`object`) }
        )
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  J. FilterDescriptor V2 features
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    fun `v2 - ImplicitFilter created with predicate and reference`() {
        val imp = FilterDescriptor.ImplicitFilter(
            predicate = p1,
            referenceSubject = s1,
            parameters = mapOf("distance" to 100.0)
        )
        assertEquals(p1, imp.predicate)
        assertEquals(s1, imp.referenceSubject)
        assertEquals(mapOf("distance" to 100.0), imp.parameters)
    }

    @Test
    fun `v2 - withImplicitFilter adds to descriptor`() {
        val d = FilterDescriptor.EMPTY.withImplicitFilter(p1, s1)
        assertEquals(1, d.implicitFilters.size)
        assertEquals(p1, d.implicitFilters[0].predicate)
    }

    @Test
    fun `v2 - withImplicitFilter increments depth`() {
        val d = FilterDescriptor.EMPTY.withImplicitFilter(p1, s1)
        assertEquals(1, d.depth)
    }

    @Test
    fun `v2 - implicitFilters make descriptor non-trivial`() {
        val d = FilterDescriptor.EMPTY.withImplicitFilter(p1, s1)
        assertFalse(d.isTrivial)
    }

    @Test
    fun `v2 - withSelectivityHint adds hint`() {
        val d = FilterDescriptor.EMPTY.withSelectivityHint(p1, 0.05)
        assertEquals(1, d.selectivityHints.size)
        assertEquals(0.05, d.selectivityHints[0].estimatedSelectivity, 0.001)
    }

    @Test
    fun `v2 - withSelectivityHint increments depth`() {
        val d = FilterDescriptor.EMPTY.withSelectivityHint(p1, 0.05)
        assertEquals(1, d.depth)
    }

    @Test
    fun `v2 - multiple withImplicitFilter accumulates`() {
        val d = FilterDescriptor.EMPTY
            .withImplicitFilter(p1, s1)
            .withImplicitFilter(p2, s2, mapOf("min" to 1.0))
        assertEquals(2, d.implicitFilters.size)
        assertEquals(2, d.depth)
    }

    @Test
    fun `v2 - isSqlPushDownCandidate true for range filter`() {
        val d = FilterDescriptor.EMPTY.withRangeFilter(p2, 0.0, 10.0)
        assertTrue(d.isSqlPushDownCandidate())
    }

    @Test
    fun `v2 - isSqlPushDownCandidate true for text filter`() {
        val d = FilterDescriptor.EMPTY.withTextFilter(pt, "test")
        assertTrue(d.isSqlPushDownCandidate())
    }

    @Test
    fun `v2 - isSqlPushDownCandidate true for large exclusion set`() {
        val bigExcluded = (1..101).map { QuadValue.of("http://ex/$it") }
        val d = FilterDescriptor.EMPTY.withExclusionFilter(p1, bigExcluded)
        assertTrue(d.isSqlPushDownCandidate())
    }

    @Test
    fun `v2 - isSqlPushDownCandidate true for ordering with limit`() {
        val d = FilterDescriptor.EMPTY.withOrdering(
            listOf(OrderSpec(QuadComponent.OBJECT, true)), limit = 10
        )
        assertTrue(d.isSqlPushDownCandidate())
    }

    @Test
    fun `v2 - isSqlPushDownCandidate false for simple set filter`() {
        val d = FilterDescriptor.EMPTY.mergeFilter(setOf(s1), null, null)
        assertFalse(d.isSqlPushDownCandidate())
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  K. ImplicitFilter end-to-end resolution
    // ═══════════════════════════════════════════════════════════════════════

    /** Shared implicit-relation test predicate */
    private val pImplicitRel = QuadValue.of("http://ex/implicitContains")

    @Test
    fun `v2 - implicitFilterResolved restricts subjects via materialize`() {
        val corpus = setOf(
            Quad(s1, p1, ou1),
            Quad(s2, p1, ou2),
            Quad(s3, p2, ou3),
            Quad(s1, pImplicitRel, s2),   // s1 -> s2 via pImplicitRel
            Quad(ou2, pImplicitRel, s1),  // ou2 -> s1 via pImplicitRel
        )
        val bqs = BasicQuadSet(corpus)
        val dqs = DeferredQuadSet(bqs, FilterDescriptor.EMPTY.withImplicitFilter(pImplicitRel, s2))
        val result = dqs.materialize()
        // pImplicitRel with refSubject=s2 → subjects where object==s2 → just s1
        // So only quads with subject s1
        assertEquals(2, result.size)
        assertTrue(result.all { it.subject == s1 })
    }

    @Test
    fun `v2 - implicitFilterResolved with no matching subjects returns empty`() {
        val corpus = setOf(
            Quad(s1, p1, ou1),
            Quad(s1, pImplicitRel, s2),  // only s1 has explicit rel, object is s2
        )
        val bqs = BasicQuadSet(corpus)
        val dqs = DeferredQuadSet(bqs, FilterDescriptor.EMPTY.withImplicitFilter(pImplicitRel, s1))
        val result = dqs.materialize()
        // source.filterPredicate(pImplicitRel) → Quad(s1,pImplicitRel,s2)
        // filter where object==s1 → none
        assertTrue(result.isEmpty())
    }

    @Test
    fun `v2 - implicitFilterResolved with existing subject filter intersection`() {
        val corpus = setOf(
            Quad(s1, p1, ou1),
            Quad(s2, p1, ou2),
            Quad(s3, p1, ou1),
            Quad(s1, pImplicitRel, s2),
            Quad(s2, pImplicitRel, s3),
            Quad(s3, pImplicitRel, s3),
        )
        val bqs = BasicQuadSet(corpus)
        val dqs = DeferredQuadSet(
            bqs,
            FilterDescriptor.EMPTY
                .mergeFilter(null, null, setOf(ou1))  // must have object ou1
                .withImplicitFilter(pImplicitRel, s3)  // must implicitly relate to s3
        )
        val result = dqs.materialize()
        // ImplicitFilter(pImplicitRel, s3): subjects where object==s3 via pImplicitRel → {s2, s3}
        // Object filter: ou1 → only quads with object ou1
        // Subjects {s2, s3} that also have object ou1 → Quad(s3, p1, ou1)
        assertEquals(1, result.size)
        assertEquals(s3, result.first().subject)
        assertEquals(p1, result.first().predicate)
        assertEquals(ou1, result.first().`object`)
    }

    @Test
    fun `v2 - multipleImplicitFilters intersected`() {
        val corpus = setOf(
            Quad(s1, p1, ou1),
            Quad(s2, p1, ou2),
            Quad(ou1, pImplicitRel, s1),  // ou1 -> s1
            Quad(s2, pImplicitRel, s1),   // s2 -> s1
            Quad(ou1, p2, s2),            // ou1 -> s2 (second implicit predicate)
            Quad(s1, p2, s2),             // s1 -> s2
        )
        val bqs = BasicQuadSet(corpus)
        val dqs = DeferredQuadSet(
            bqs,
            FilterDescriptor.EMPTY
                .withImplicitFilter(pImplicitRel, s1)   // subjects related to s1 via pImplicitRel → {ou1, s2}
                .withImplicitFilter(p2, s2)             // subjects related to s2 via p2 → {ou1, s1}
                // Intersection of {ou1, s2} and {ou1, s1} → {ou1}
        )
        val result = dqs.materialize()
        // all quads with subject = ou1
        assertEquals(2, result.size)
        assertTrue(result.all { it.subject == ou1 })
    }

    @Test
    fun `v2 - implicitFilterResolved after deferred composition`() {
        val corpus = setOf(
            Quad(s1, p1, ou1),
            Quad(s2, p1, ou2),
            Quad(ou1, p1, ou3),
            Quad(s1, pImplicitRel, s2),   // s1 -> s2 via pImplicitRel
            Quad(s2, pImplicitRel, s2),   // s2 -> s2 via pImplicitRel
        )
        val bqs = BasicQuadSet(corpus)
        val dqs = DeferredQuadSet
            .from(bqs)
            .filterPredicate(p1) as DeferredQuadSet
        // Manually add ImplicitFilter via descriptor copy
        val withImpl = DeferredQuadSet(
            bqs,
            dqs.descriptor.copy(
                implicitFilters = listOf(
                    FilterDescriptor.ImplicitFilter(pImplicitRel, s2)
                )
            )
        )
        val result = withImpl.materialize()
        // ImplicitFilter: subjects where object==s2 → {s1, s2}
        // Predicate filter: p1
        // Result: Quad(s1, p1, ou1), Quad(s2, p1, ou2)
        assertEquals(2, result.size)
    }
}
