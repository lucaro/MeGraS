# DeferredQuadSet V2 Design Document

## Executive Summary

V1 delivered transducer-based filter composition with single-SQL push-down for PostgresStore. V2 extends this with:
1. **Shard-level combined filter** — push FilterDescriptor to each shard for local execution
2. **Hybrid execution** — selective push-down based on filter selectivity
3. **Implicit relation integration** — spatial/temporal/knn as composable filter entries
4. **Statistics-driven optimization** — use table stats for join order and push-down decisions

---

## 1. Core Interface Changes

### 1.1 FilterDescriptor Extensions

```kotlin
// Add to FilterDescriptor.kt
data class FilterDescriptor(
    // ... existing fields ...
    val implicitFilters: List<ImplicitFilter> = emptyList(),
    val selectivityHints: Map<String, Double> = emptyMap()  // predicate -> estimated selectivity
) {
    data class ImplicitFilter(
        val predicate: QuadValue,           // e.g., SPATIAL_CONTAINS, TEMPORAL_PRECEDES
        val referenceSubject: QuadValue,    // the anchor subject
        val parameters: Map<String, Any>    // e.g., { "distance": 100.0, "unit": "meters" }
    )
    
    fun withImplicitFilter(predicate: QuadValue, referenceSubject: QuadValue, parameters: Map<String, Any>): FilterDescriptor =
        copy(implicitFilters = implicitFilters + ImplicitFilter(predicate, referenceSubject, parameters), depth = depth + 1)
}
```

### 1.2 Shard Interface Extension

```kotlin
// Add to Shard.kt
interface Shard {
    // ... existing methods ...
    
    /**
     * Execute a composed filter locally on this shard.
     * Returns matching row ids. All operands are already resolved to QuadValueIds.
     * 
     * @param descriptor FilterDescriptor with all conditions pre-resolved to IDs
     * @param scalarIds Map of QuadValue -> QuadValueId for scalar operands
     * @param vectorIds Map of VectorValue -> QuadValueId for vector operands (this shard only)
     */
    fun filterCombined(
        descriptor: FilterDescriptor,
        scalarIds: Map<QuadValue, QuadValueId>,
        vectorIds: Map<VectorValue, QuadValueId>
    ): Set<Long>
}
```

### 1.3 ClusterQuadSet.materializeFilter()

```kotlin
// Add to ClusterQuadSet.kt
override fun materializeFilter(descriptor: FilterDescriptor): QuadSet {
    if (descriptor.isImpossible) return BasicQuadSet()
    if (descriptor.isTrivial) return this as QuadSet
    
    // 1. Resolve ALL filter operands to IDs (scalar + vector)
    val allFilterValues = collectAllFilterValues(descriptor)
    val scalarIds = resolveScalarValues(allFilterValues.filter { it !is VectorValue })
    val vectorIdsByShard = resolveVectorValuesByShard(allFilterValues.filter { it is VectorValue })
    
    // 2. Fan out to relevant shards with the SAME descriptor
    val shardTuples = ArrayList<Pair<Shard, Set<Triple<QuadValueId, QuadValueId, QuadValueId>>>>()
    val contactShards = determineContactShards(descriptor, scalarIds, vectorIdsByShard)
    
    for (shard in contactShards) {
        val localVectorIds = vectorIdsByShard[shard] ?: emptyMap()
        val rowIds = shard.filterCombined(descriptor, scalarIds, localVectorIds)
        if (rowIds.isNotEmpty()) {
            val tuples = shard.quadTuples(rowIds).values.toSet()
            if (tuples.isNotEmpty()) shardTuples.add(shard to tuples)
        }
    }
    
    return resolveQuadSet(shardTuples)
}
```

---

## 2. SQL Generation Improvements (PostgresStore)

### 2.1 CTE-Based Composition

```kotlin
// In buildMaterializedSql(), use CTEs for complex filters
private fun buildMaterializedSql(descriptor: FilterDescriptor, filterIds: Map<QuadValue, QuadValueId>): String {
    val sb = StringBuilder()
    
    // Base CTE: all quads matching core set filters
    sb.append("WITH base AS (")
    sb.append("SELECT q.id, q.s_type, q.s, q.p_type, q.p, q.o_type, q.o FROM quads q")
    appendCoreFilters(sb, descriptor, filterIds)
    sb.append(")")
    
    // Range filter CTE (if any)
    if (descriptor.rangeFilters.isNotEmpty()) {
        sb.append(", ranged AS (")
        sb.append("SELECT * FROM base")
        sb.append(" INNER JOIN literal_double d ON base.o = d.id AND base.o_type = $DOUBLE_LITERAL_TYPE")
        appendRangeFilters(sb, descriptor, filterIds)
        sb.append(")")
    }
    
    // Text filter CTE (if any)
    if (descriptor.textFilters.isNotEmpty()) {
        sb.append(", texted AS (")
        sb.append("SELECT * FROM ${if (descriptor.rangeFilters.isNotEmpty()) "ranged" else "base"}")
        appendTextFilters(sb, descriptor, filterIds)
        sb.append(")")
    }
    
    // Exclusion CTE (if any)
    if (descriptor.exclusionFilters.isNotEmpty()) {
        sb.append(", filtered AS (")
        sb.append("SELECT * FROM ${lastCteName}")
        appendExclusionFilters(sb, descriptor, filterIds)
        sb.append(")")
    }
    
    // Final select with ORDER BY / LIMIT / OFFSET
    sb.append("SELECT * FROM ${lastCteName}")
    appendOrderByLimitOffset(sb, descriptor)
    
    return sb.toString()
}
```

### 2.2 Selectivity-Based Join Order

```kotlin
// Estimate selectivity from table statistics
private fun estimateSelectivity(predicate: QuadValue, filterIds: Map<QuadValue, QuadValueId>): Double {
    val pId = filterIds[predicate] ?: return 1.0
    // Query pg_stats or use cached estimates
    // SELECT n_distinct FROM pg_stats WHERE tablename='quads' AND attname='p'
    // Return 1.0 / n_distinct as selectivity estimate
}
```

---

## 3. Distributed Execution Model

### 3.1 Shard-Local Combined Filter

Each shard implements `filterCombined()` by building a local SQL query equivalent to PostgresStore's `buildMaterializedSql()` but operating on its local `quads` table.

```kotlin
// In PostgresShard.kt (implements Shard)
override fun filterCombined(
    descriptor: FilterDescriptor,
    scalarIds: Map<QuadValue, QuadValueId>,
    vectorIds: Map<VectorValue, QuadValueId>
): Set<Long> {
    // Build SQL using same logic as PostgresStore.buildMaterializedSql()
    // but all IDs are already resolved, no dictionary lookups needed
    val sql = buildShardSql(descriptor, scalarIds, vectorIds)
    return transaction { exec(sql) { rs -> rs.getLong("id") }.toSet() }
}
```

### 3.2 Contact Shard Determination

```kotlin
// In ClusterQuadSet
private fun determineContactShards(
    descriptor: FilterDescriptor,
    scalarIds: Map<QuadValue, QuadValueId>,
    vectorIdsByShard: Map<Shard, Map<VectorValue, QuadValueId>>
): Set<Shard> {
    val contact = mutableSetOf<Shard>()
    
    // Scalar operands broadcast to all shards
    if (descriptor.subjects?.isNotEmpty() == true) contact.addAll(policy.allShards())
    if (descriptor.predicates?.isNotEmpty() == true) contact.addAll(policy.allShards())
    if (descriptor.objects?.isNotEmpty() == true) contact.addAll(policy.allShards())
    
    // Vector operands route to their content shards
    contact.addAll(vectorIdsByShard.keys)
    
    // Range/text/exclusion on predicate -> broadcast (predicate is scalar)
    if (descriptor.rangeFilters.isNotEmpty()) contact.addAll(policy.allShards())
    if (descriptor.textFilters.isNotEmpty()) contact.addAll(policy.allShards())
    if (descriptor.exclusionFilters.isNotEmpty()) contact.addAll(policy.allShards())
    
    // Implicit filters: route based on reference subject
    for (imp in descriptor.implicitFilters) {
        val refId = scalarIds[imp.referenceSubject] ?: continue
        val definite = policy.definiteShard(imp.referenceSubject, refId.first, refId.second, 0, 0)
        if (definite != null) contact.add(definite) else contact.addAll(policy.allShards())
    }
    
    return contact
}
```

---

## 4. Hybrid Execution Strategy

### 4.1 Push-Down Decision

```kotlin
// In DeferredQuadSet.materialize()
override fun materialize(): QuadSet {
    if (descriptor.isImpossible) return BasicQuadSet()
    if (descriptor.isTrivial) return source
    
    // Decide: push to SQL or execute in-memory?
    val shouldPushDown = when (source) {
        is AbstractDbStore -> shouldPushDownToDb(descriptor)
        is ClusterQuadSet -> shouldPushDownToCluster(descriptor)
        else -> false
    }
    
    return if (shouldPushDown) {
        source.materializeFilter(descriptor)
    } else {
        materializeViaIterator()
    }
}

private fun shouldPushDownToDb(descriptor: FilterDescriptor): Boolean {
    // Push down if:
    // - Any range filter (needs SQL join)
    // - Any text filter (needs full-text index)
    // - Estimated result size > 1000 (avoid materializing large sets in memory)
    // - Has ordering with limit (SQL LIMIT is efficient)
    descriptor.rangeFilters.isNotEmpty() ||
    descriptor.textFilters.isNotEmpty() ||
    descriptor.exclusionFilters.any { it.excludedValues.size > 100 } ||
    (descriptor.orderBy.isNotEmpty() && descriptor.limit < Int.MAX_VALUE) ||
    estimateResultSize(descriptor) > 1000
}
```

### 4.2 Partial Push-Down

For mixed workloads, push selective filters to SQL, keep others in-memory:

```kotlin
// Split descriptor into pushable and non-pushable parts
data class SplitDescriptor(
    val pushable: FilterDescriptor,      // goes to SQL
    val residual: FilterDescriptor       // applied in-memory after SQL
)

fun splitForHybrid(descriptor: FilterDescriptor, source: QuadSet): SplitDescriptor {
    val pushable = mutableListOf<FilterEntry>()
    val residual = mutableListOf<FilterEntry>()
    
    // Core set filters: push if selective
    descriptor.subjects?.let { if (it.size < 1000) pushable.add(Subjects(it)) else residual.add(Subjects(it)) }
    descriptor.predicates?.let { if (it.size < 100) pushable.add(Predicates(it)) else residual.add(Predicates(it)) }
    descriptor.objects?.let { if (it.size < 1000) pushable.add(Objects(it)) else residual.add(Objects(it)) }
    
    // Range/text/exclusion: always push (need SQL)
    descriptor.rangeFilters.forEach { pushable.add(Range(it)) }
    descriptor.textFilters.forEach { pushable.add(Text(it)) }
    descriptor.exclusionFilters.forEach { pushable.add(Exclusion(it)) }
    
    // Ordering/limit: push if limit is small
    if (descriptor.orderBy.isNotEmpty() && descriptor.limit < 10000) {
        pushable.add(Ordering(descriptor.orderBy, descriptor.limit, descriptor.offset))
    } else {
        residual.add(Ordering(descriptor.orderBy, descriptor.limit, descriptor.offset))
    }
    
    return SplitDescriptor(
        FilterDescriptor.fromEntries(pushable),
        FilterDescriptor.fromEntries(residual)
    )
}
```

---

## 5. Implicit Relation Integration

### 5.1 Implicit Filter Entry

```kotlin
// In FilterDescriptor.kt
data class ImplicitFilter(
    val predicate: QuadValue,           // e.g., SPATIAL_CONTAINS, TEMPORAL_PRECEDES
    val referenceSubject: QuadValue,    // the anchor subject
    val parameters: Map<String, Any>    // e.g., { "distance": 100.0, "unit": "meters" }
)

// In DeferredQuadSet
override fun implicitFilter(predicate: QuadValue, referenceSubject: QuadValue, parameters: Map<String, Any>): QuadSet {
    if (descriptor.depth >= CIRCUIT_BREAKER_DEPTH) {
        return materialize().implicitFilter(predicate, referenceSubject, parameters)
    }
    return DeferredQuadSet(source, descriptor.withImplicitFilter(predicate, referenceSubject, parameters))
}
```

### 5.2 Shard-Level Implicit Execution

```kotlin
// In Shard interface
fun implicitFilter(
    predicate: QuadValueId,
    referenceSubject: QuadValueId,
    parameters: Map<String, Any>
): Set<Long>

// In PostgresShard: delegate to existing implicit handlers but at ID level
// In ClusterQuadSet: route to shards owning the reference subject
```

---

## 6. Circuit Breaker Refinements

### 6.1 Adaptive Thresholds

```kotlin
// In FilterDescriptor companion object
const val CIRCUIT_BREAKER_DEPTH = 5
const val CIRCUIT_BREAKER_SET_SIZE = 2000

// Dynamic thresholds based on source
fun circuitBreakerDepth(source: QuadSet): Int = when (source) {
    is AbstractDbStore -> 10  // DB can handle deeper composition
    is ClusterQuadSet -> 8    // Network overhead, but parallel
    else -> 5
}

fun circuitBreakerSetSize(source: QuadSet): Int = when (source) {
    is AbstractDbStore -> 5000  // SQL IN clauses handle larger sets
    is ClusterQuadSet -> 3000   // Broadcast cost
    else -> 2000
}
```

### 6.2 Selectivity-Aware Breaking

```kotlin
// Break if estimated result size exceeds threshold
fun shouldBreakForSelectivity(descriptor: FilterDescriptor, source: QuadSet): Boolean {
    val estimated = estimateResultSize(descriptor, source)
    return estimated > (source as? AbstractDbStore)?.estimatedTableSize?.times(0.1) ?: 10000
}
```

---

## 7. Test Strategy

### 7.1 Unit Tests (FilterDescriptor)
- Implicit filter composition
- Selectivity hint propagation
- SplitDescriptor correctness

### 7.2 Integration Tests (PostgresStore)
- CTE-based SQL generation
- Hybrid execution path
- Selectivity estimation accuracy

### 7.3 Distributed Tests (ClusterQuadSet)
- filterCombined on single shard
- filterCombined across multiple shards
- Scalar broadcast + vector routing
- Implicit filter routing

### 7.4 Parity Tests
- DeferredQuadSet + BasicQuadSet = DeferredQuadSet + PostgresStore = DeferredQuadSet + ClusterQuadSet
- All filter combinations produce identical results

---

## 8. Implementation Phases

| Phase | Scope | Files |
|-------|-------|-------|
| 2.1 | FilterDescriptor + ImplicitFilter | FilterDescriptor.kt, DeferredQuadSet.kt |
| 2.2 | Shard.filterCombined() | Shard.kt, PostgresShard.kt |
| 2.3 | ClusterQuadSet.materializeFilter() | ClusterQuadSet.kt |
| 2.4 | CTE-based SQL + hybrid execution | PostgresStore.kt, DeferredQuadSet.kt |
| 2.5 | Selectivity estimation | PostgresStore.kt (new StatisticsService) |
| 2.6 | Implicit filter integration | Implicit handlers, Shard.kt, ClusterQuadSet.kt |
| 2.7 | Tests | DeferredQuadSetCorrectnessTest.kt, new ClusterParityTest.kt |

---

## 9. Backward Compatibility

- All V1 APIs remain unchanged
- FilterDescriptor adds optional fields (default empty)
- Shard.filterCombined() has default implementation that throws UnsupportedOperationException
- ClusterQuadSet.materializeFilter() falls back to sequential replay if shards don't support combined filter
- Circuit breaker thresholds are configurable via system properties

---

## 10. Open Questions

1. **Statistics collection**: How often to refresh pg_stats? Background job or on-demand?
2. **Vector filter push-down**: Can pgvector index be used in combined filter? (requires `ORDER BY ... LIMIT` in CTE)
3. **Implicit filter parameters**: Standardize parameter schema across spatial/temporal/knn?
4. **Partial push-down rollback**: If SQL push-down fails, gracefully fall back to in-memory?
5. **Distributed transaction**: materializeFilter() is read-only, but what about future write paths?