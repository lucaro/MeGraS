package org.megras.graphstore.db

import com.google.common.cache.CacheBuilder
import org.megras.data.graph.*
import org.megras.graphstore.MutableQuadSet
import org.megras.graphstore.QuadSet
import org.megras.graphstore.deferred.FilterDescriptor
import org.megras.id.id
import org.megras.util.extensions.toBase64
import java.io.Writer
import java.nio.ByteBuffer

abstract class AbstractDbStore : MutableQuadSet {

    companion object {
        const val QUAD_TYPE = 0
        const val LOCAL_URI_TYPE = -1
        const val LONG_LITERAL_TYPE = -2
        const val DOUBLE_LITERAL_TYPE = -3
        const val STRING_LITERAL_TYPE = -4

        const val BINARY_DATA_TYPE = -9
        const val VECTOR_ID_OFFSET = -10

        const val cacheSize = 1000000L
    }


    private val stringLiteralIdCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build<String, Long>()
    private val stringLiteralValueCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build<Long, String>()

    private val doubleLiteralIdCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build<Double, Long>()
    private val doubleLiteralValueCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build<Long, Double>()

    private val prefixValueCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build<Int, String>()
    private val prefixIdCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build<String, Int>()

    private val suffixValueCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build<Long, String>()
    private val suffixIdCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build<String, Long>()

    private val uriValueIdCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build<QuadValueId, URIValue>()
    private val uriValueValueCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build<URIValue, QuadValueId>()


    private val vectorValueIdCache =
        CacheBuilder.newBuilder().maximumSize(cacheSize).build<QuadValueId, VectorValue>()
    private val vectorValueValueCache =
        CacheBuilder.newBuilder().maximumSize(cacheSize).build<VectorValue, QuadValueId>()


    abstract fun setup()

    protected abstract fun lookUpDoubleValueIds(doubleValues: Set<DoubleValue>): Map<DoubleValue, QuadValueId>
    protected abstract fun lookUpStringValueIds(stringValues: Set<StringValue>): Map<StringValue, QuadValueId>
    protected abstract fun lookUpPrefixIds(prefixValues: Set<String>): Map<String, Int>
    protected abstract fun lookUpSuffixIds(suffixValues: Set<String>): Map<String, Long>
    protected abstract fun lookUpVectorValueIds(vectorValues: Set<VectorValue>): Map<VectorValue, QuadValueId>


    protected abstract fun insertDoubleValues(doubleValues: Set<DoubleValue>): Map<DoubleValue, QuadValueId>
    protected abstract fun insertStringValues(stringValues: Set<StringValue>): Map<StringValue, QuadValueId>
    protected abstract fun insertPrefixValues(prefixValues: Set<String>): Map<String, Int>
    protected abstract fun insertSuffixValues(suffixValues: Set<String>): Map<String, Long>
    protected abstract fun insertVectorValueIds(vectorValues: Set<VectorValue>): Map<VectorValue, QuadValueId>

    fun getQuadValueId(quadValue: QuadValue): Pair<Int?, Long?> {

        return when (quadValue) {
            is DoubleValue -> {
                val cached = doubleLiteralIdCache.getIfPresent(quadValue.value)
                if (cached != null) {
                    DOUBLE_LITERAL_TYPE to cached
                } else {
                    val lookup = lookUpDoubleValueIds(setOf(quadValue))[quadValue]?.second
                    if (lookup != null) {
                        doubleLiteralIdCache.put(quadValue.value, lookup)
                        doubleLiteralValueCache.put(lookup, quadValue.value)
                    }
                    DOUBLE_LITERAL_TYPE to lookup
                }

            }
            is LongValue -> LONG_LITERAL_TYPE to quadValue.value //no indirection needed
            is StringValue -> {
                val cached = stringLiteralIdCache.getIfPresent(quadValue.value)
                if (cached != null) {
                    STRING_LITERAL_TYPE to cached
                } else {
                    val lookup = lookUpStringValueIds(setOf(quadValue))[quadValue]?.second
                    if (lookup != null) {
                        stringLiteralIdCache.put(quadValue.value, lookup)
                        stringLiteralValueCache.put(lookup, quadValue.value)
                    }
                    STRING_LITERAL_TYPE to lookup
                }

            }
            is URIValue -> {

                val cachedType = if (quadValue is LocalQuadValue) LOCAL_URI_TYPE else prefixIdCache.getIfPresent(quadValue.prefix())
                val cachedSuffix = suffixIdCache.getIfPresent(quadValue.suffix())

                if (cachedType != null && cachedSuffix != null) {
                    return cachedType to cachedSuffix
                }

                val type = cachedType ?: lookUpPrefixIds(setOf(quadValue.prefix()))[quadValue.prefix()]
                val suffix = cachedSuffix ?: lookUpSuffixIds(setOf(quadValue.suffix()))[quadValue.suffix()]

                if (quadValue !is LocalQuadValue && type != null) {
                    prefixIdCache.put(quadValue.prefix(), type)
                    prefixValueCache.put(type, quadValue.prefix())
                }

                if (suffix != null) {
                    suffixIdCache.put(quadValue.suffix(), suffix)
                    suffixValueCache.put(suffix, quadValue.suffix())
                }

                type to suffix
            }
            is VectorValue -> {

                val cached = vectorValueValueCache.getIfPresent(quadValue)

                if (cached != null) {
                    return cached
                }

                val lookup = lookUpVectorValueIds(setOf(quadValue))[quadValue]

                if (lookup != null) {
                    vectorValueValueCache.put(quadValue, lookup)
                    vectorValueIdCache.put(lookup, quadValue)
                }

                lookup?.first to lookup?.second
            }
            is TemporalValue -> {
                TODO("Not yet implemented")
            }
        }

    }


    fun getOrAddQuadValueId(quadValue: QuadValue): QuadValueId {

        return when (quadValue) {
            is DoubleValue -> {
                val cached = doubleLiteralIdCache.getIfPresent(quadValue.value)
                if (cached != null) {
                    DOUBLE_LITERAL_TYPE to cached
                } else {
                    val set = setOf(quadValue)
                    val lookup = lookUpDoubleValueIds(set)[quadValue]?.second
                    if (lookup != null) {
                        doubleLiteralIdCache.put(quadValue.value, lookup)
                        DOUBLE_LITERAL_TYPE to lookup
                    } else {
                        val id = insertDoubleValues(set)[quadValue]!!
                        doubleLiteralValueCache.put(id.second, quadValue.value)
                        doubleLiteralIdCache.put(quadValue.value, id.second)
                        id
                    }
                }
            }
            is LongValue -> LONG_LITERAL_TYPE to quadValue.value //no indirection needed
            is StringValue -> {
                val cached = stringLiteralIdCache.getIfPresent(quadValue.value)
                if (cached != null) {
                    STRING_LITERAL_TYPE to cached
                } else {
                    val set = setOf(quadValue)
                    val lookup = lookUpStringValueIds(set)[quadValue]?.second
                    if (lookup != null) {
                        stringLiteralIdCache.put(quadValue.value, lookup)
                        stringLiteralValueCache.put(lookup, quadValue.value)
                        STRING_LITERAL_TYPE to lookup
                    } else {
                        val id = insertStringValues(set)[quadValue]!!
                        stringLiteralIdCache.put(quadValue.value, id.second)
                        stringLiteralValueCache.put(id.second, quadValue.value)
                        STRING_LITERAL_TYPE to id.second
                    }
                }
            }
            is URIValue -> {

                val type = if (quadValue is LocalQuadValue) {
                    LOCAL_URI_TYPE
                } else {
                    val cachedType = prefixIdCache.getIfPresent(quadValue.prefix())
                    if (cachedType != null) {
                        cachedType
                    } else {
                        val set = setOf(quadValue.prefix())
                        val lookupType = lookUpPrefixIds(set)[quadValue.prefix()]
                        if (lookupType != null) {
                            prefixIdCache.put(quadValue.prefix(), lookupType)
                            prefixValueCache.put(lookupType, quadValue.prefix())
                            lookupType
                        } else {
                            val insertType = insertPrefixValues(set)[quadValue.prefix()]!!
                            prefixIdCache.put(quadValue.prefix(), insertType)
                            prefixValueCache.put(insertType, quadValue.prefix())
                            insertType
                        }
                    }
                }

                val cachedSuffix = suffixIdCache.getIfPresent(quadValue.suffix())

                val suffix = if (cachedSuffix != null) {
                    cachedSuffix
                } else {
                    val set = setOf(quadValue.suffix())
                    val lookup = lookUpSuffixIds(set)[quadValue.suffix()]
                    if (lookup != null) {
                        suffixIdCache.put(quadValue.suffix(), lookup)
                        suffixValueCache.put(lookup, quadValue.suffix())
                        lookup
                    } else {
                        val insert = insertSuffixValues(set)[quadValue.suffix()]!!
                        suffixIdCache.put(quadValue.suffix(), insert)
                        suffixValueCache.put(insert, quadValue.suffix())
                        insert
                    }
                }

                type to suffix

            }
            is VectorValue -> {

                val cached = vectorValueValueCache.getIfPresent(quadValue)

                if (cached != null) {
                    return cached
                }

                val set = setOf(quadValue)

                val lookup = lookUpVectorValueIds(set)[quadValue]

                if (lookup != null) {
                    vectorValueValueCache.put(quadValue, lookup)
                    vectorValueIdCache.put(lookup, quadValue)
                    return lookup
                }

                val insert = insertVectorValueIds(set)[quadValue]!!
                vectorValueValueCache.put(quadValue, insert)
                vectorValueIdCache.put(insert, quadValue)

                insert
            }
            is TemporalValue -> {
                TODO("Not yet implemented")
            }
        }

    }

    fun getQuadValue(type: Int, id: Long): QuadValue? {

        return when {
            type == DOUBLE_LITERAL_TYPE -> {
                val cached = doubleLiteralValueCache.getIfPresent(id)
                if (cached != null) {
                    return DoubleValue(cached)
                }
                val value = lookUpDoubleValues(setOf(id))[DOUBLE_LITERAL_TYPE to id]
                if (value != null) {
                    doubleLiteralIdCache.put(value.value, id)
                    doubleLiteralValueCache.put(id, value.value)
                }
                value
            }
            type == LONG_LITERAL_TYPE -> LongValue(id) //no indirection needed
            type == STRING_LITERAL_TYPE -> {
                val cached = stringLiteralValueCache.getIfPresent(id)
                if (cached != null) {
                    return StringValue(cached)
                }
                val value = lookUpStringValues(setOf(id))[STRING_LITERAL_TYPE to id]
                if (value != null) {
                    stringLiteralIdCache.put(value.value, id)
                    stringLiteralValueCache.put(id, value.value)
                }
                value
            }
            type < VECTOR_ID_OFFSET -> {
                val quadId = type to id
                val cached = vectorValueIdCache.getIfPresent(quadId)
                if (cached != null) {
                    return cached
                }

                val value = lookUpVectorValues(setOf(quadId))[quadId]
                if (value != null) {
                    vectorValueValueCache.put(value, quadId)
                    vectorValueIdCache.put(quadId, value)
                }
                value
            }
            else -> {
                val quadId = type to id
                val cached = uriValueIdCache.getIfPresent(quadId)

                if (cached != null) {
                    return cached
                }

                if (type == LOCAL_URI_TYPE) {

                    val suffix = suffixValueCache.getIfPresent(id) ?: lookUpSuffixes(setOf(id))[id] ?: return null

                    suffixValueCache.put(id, suffix)
                    suffixIdCache.put(suffix, id)

                    val value = LocalQuadValue(suffix)
                    uriValueIdCache.put(quadId, value)
                    value

                } else {

                    val prefix = prefixValueCache.getIfPresent(type) ?: lookUpPrefixes(setOf(type))[type]
                    if (prefix != null) {
                        prefixValueCache.put(type, prefix)
                        prefixIdCache.put(prefix, type)
                    } else {
                        return null
                    }

                    val suffix = suffixValueCache.getIfPresent(id) ?: lookUpSuffixes(setOf(id))[id] ?: return null

                    suffixValueCache.put(id, suffix)
                    suffixIdCache.put(suffix, id)

                    val value = URIValue(prefix, suffix)
                    uriValueIdCache.put(quadId, value)
                    value

                }
            }
        }

    }

    fun getQuadValues(ids: Collection<QuadValueId>): Map<QuadValueId, QuadValue> {

        val returnMap = mutableMapOf<QuadValueId, QuadValue>()
        val uriIds = HashSet<QuadValueId>()

        ids.groupBy { it.first }.forEach { (type, groupedPairs) ->

            when {
                type == DOUBLE_LITERAL_TYPE -> {
                    val pairSet = groupedPairs.toMutableSet()
                    pairSet.removeIf {
                        val cached = doubleLiteralValueCache.getIfPresent(it.second)
                        if (cached != null) {
                            returnMap[it] = DoubleValue(cached)
                            true
                        } else {
                            false
                        }
                    }
                    if (pairSet.isNotEmpty()) {

                        val map = lookUpDoubleValues(groupedPairs.map { it.second }.toSet())

                        map.forEach { (id, value) ->
                            doubleLiteralIdCache.put(value.value, id.second)
                            doubleLiteralValueCache.put(id.second, value.value)
                        }

                        returnMap.putAll(map)
                    }
                }

                type == LONG_LITERAL_TYPE -> {
                    returnMap.putAll(groupedPairs.associateWith { LongValue(it.second) })
                }

                type == STRING_LITERAL_TYPE -> {
                    val pairSet = groupedPairs.toMutableSet()
                    pairSet.removeIf {
                        val cached = stringLiteralValueCache.getIfPresent(it.second)
                        if (cached != null) {
                            returnMap[it] = StringValue(cached)
                            true
                        } else {
                            false
                        }
                    }

                    if (pairSet.isNotEmpty()) {

                        val map = lookUpStringValues(pairSet.map { it.second }.toSet())

                        map.forEach { (id, value) ->
                            stringLiteralValueCache.put(id.second, value.value)
                            stringLiteralIdCache.put(value.value, id.second)
                        }

                        returnMap.putAll(map)

                    }
                }

                type < VECTOR_ID_OFFSET -> {

                    val map = lookUpVectorValues(groupedPairs.toSet())

                    map.forEach { (id, value) ->
                        vectorValueValueCache.put(value, id)
                        vectorValueIdCache.put(id, value)
                    }

                    returnMap.putAll(map)

                }

                else -> uriIds.addAll(groupedPairs)
            }

        }

        val prefixes = lookUpPrefixes(uriIds.map { it.first }.filter { it != LOCAL_URI_TYPE }.toSet())
        val suffixes = lookUpSuffixes(uriIds.map { it.second }.toSet())

        returnMap.putAll(
            uriIds.mapNotNull {
                val suffix = suffixes[it.second] ?: return@mapNotNull null
                if (it.first == LOCAL_URI_TYPE) {
                    it to LocalQuadValue(suffix)
                } else {
                    val prefix = prefixes[it.first] ?: return@mapNotNull null
                    it to URIValue(prefix, suffix)
                }
            }
        )


        return returnMap

    }

    abstract fun lookUpDoubleValues(ids: Set<Long>) : Map<QuadValueId, DoubleValue>
    abstract fun lookUpStringValues(ids: Set<Long>) : Map<QuadValueId, StringValue>
    abstract fun lookUpVectorValues(ids: Set<QuadValueId>) : Map<QuadValueId, VectorValue>

    abstract fun lookUpPrefixes(ids: Set<Int>) : Map<Int, String>
    abstract fun lookUpSuffixes(ids: Set<Long>) : Map<Long, String>

    fun getOrAddQuadValueIds(
        quadValues: Collection<QuadValue>,
        insertRemaining: Boolean = true
    ): Map<QuadValue, QuadValueId> {

        if (quadValues.isEmpty()) {
            return emptyMap()
        }

        val returnMap = mutableMapOf<QuadValue, QuadValueId>()

        val doubleValues = mutableSetOf<DoubleValue>()
        val stringValues = mutableSetOf<StringValue>()
        val uriValues = mutableSetOf<URIValue>()
        val vectorValues = mutableSetOf<VectorValue>()

        //sort by type
        quadValues.forEach {
            when (it) {
                is DoubleValue -> doubleValues.add(it)
                is LongValue -> returnMap[it] = LONG_LITERAL_TYPE to it.value
                is StringValue -> stringValues.add(it)
                is URIValue -> uriValues.add(it)
                is VectorValue -> vectorValues.add(it)
                is TemporalValue -> {
                    TODO("Not yet implemented")
                }
            }
        }

        //cache lookup
        doubleValues.removeIf {
            val cached = doubleLiteralIdCache.getIfPresent(it) ?: return@removeIf false
            returnMap[it] = DOUBLE_LITERAL_TYPE to cached
            true
        }

        stringValues.removeIf {
            val cached = stringLiteralIdCache.getIfPresent(it) ?: return@removeIf false
            returnMap[it] = STRING_LITERAL_TYPE to cached
            true
        }

        uriValues.removeIf {
            val cached = uriValueValueCache.getIfPresent(it) ?: return@removeIf false
            returnMap[it] = cached
            true
        }

        vectorValues.removeIf {
            val cached = vectorValueValueCache.getIfPresent(it) ?: return@removeIf false
            returnMap[it] = cached
            true
        }


        //database lookup

        if (doubleValues.isNotEmpty()) {

            val map = lookUpDoubleValueIds(doubleValues)

            map.forEach { (value, id) ->
                doubleLiteralValueCache.put(id.second, value.value)
                doubleLiteralIdCache.put(value.value, id.second)
                doubleValues.remove(value)
            }

            returnMap.putAll(map)

            if (insertRemaining) {

                val inserted = insertDoubleValues(doubleValues)

                inserted.forEach { (value, id) ->
                    doubleLiteralValueCache.put(id.second, value.value)
                    doubleLiteralIdCache.put(value.value, id.second)
                }

                returnMap.putAll(inserted)

            }

        }


        if (stringValues.isNotEmpty()) {

            val map = lookUpStringValueIds(stringValues)

            map.forEach { (value, id) ->
                stringLiteralValueCache.put(id.second, value.value)
                stringLiteralIdCache.put(value.value, id.second)
                stringValues.remove(value)
            }

            returnMap.putAll(map)

            if (insertRemaining) {

                val inserted = insertStringValues(stringValues)

                inserted.forEach { (value, id) ->
                    stringLiteralValueCache.put(id.second, value.value)
                    stringLiteralIdCache.put(value.value, id.second)
                }

                returnMap.putAll(inserted)

            }

        }

        if (uriValues.isNotEmpty()) {

            val prefixValues =
                uriValues.asSequence().filter { it !is LocalQuadValue }.map { it.prefix() }.toMutableSet()
            val suffixValues = uriValues.map { it.suffix() }.toMutableSet()

            val prefixIdMap = mutableMapOf<String, Int>()
            val suffixIdMap = mutableMapOf<String, Long>()

            prefixValues.removeIf {
                val cached = prefixIdCache.getIfPresent(it) ?: return@removeIf false
                prefixIdMap[it] = cached
                true
            }

            suffixValues.removeIf {
                val cached = suffixIdCache.getIfPresent(it) ?: return@removeIf false
                suffixIdMap[it] = cached
                true
            }

            if (prefixValues.isNotEmpty()) {

                val map = lookUpPrefixIds(prefixValues)

                map.forEach { (value, id) ->
                    prefixValueCache.put(id, value)
                    prefixIdCache.put(value, id)
                    prefixValues.remove(value)
                }

                prefixIdMap.putAll(map)

                if (insertRemaining) {

                    val inserted = insertPrefixValues(prefixValues)

                    inserted.forEach { (value, id) ->
                        prefixValueCache.put(id, value)
                        prefixIdCache.put(value, id)
                    }

                    prefixIdMap.putAll(inserted)

                }
            }

            if (suffixValues.isNotEmpty()) {

                val map = lookUpSuffixIds(suffixValues)

                map.forEach { (value, id) ->
                    suffixIdCache.put(value, id)
                    suffixValueCache.put(id, value)
                    suffixValues.remove(value)
                }

                suffixIdMap.putAll(map)

                if (insertRemaining) {

                    val inserted = insertSuffixValues(suffixValues)

                    inserted.forEach { (value, id) ->
                        suffixIdCache.put(value, id)
                        suffixValueCache.put(id, value)
                    }

                    suffixIdMap.putAll(inserted)

                }
            }

            //combine entries
            uriValues.forEach {

                val prefix = if (it is LocalQuadValue) LOCAL_URI_TYPE else prefixIdMap[it.prefix()]
                val suffix = suffixIdMap[it.suffix()]

                if (prefix != null && suffix != null) {
                    returnMap[it] = prefix to suffix
                }

            }
        }


        if (vectorValues.isNotEmpty()) {

            val map = lookUpVectorValueIds(vectorValues)

            map.forEach { (value, id) ->
                vectorValueValueCache.put(value, id)
                vectorValueIdCache.put(id, value)
                vectorValues.remove(value)
            }

            returnMap.putAll(map)

            if (insertRemaining) {

                val inserted = insertVectorValueIds(vectorValues)

                inserted.forEach { (value, id) ->
                    vectorValueValueCache.put(value, id)
                    vectorValueIdCache.put(id, value)
                }

                returnMap.putAll(inserted)

            }
        }

        return returnMap
    }


    protected abstract fun insert(s: QuadValueId, p: QuadValueId, o: QuadValueId, semid: String) : Long
    abstract fun getQuadId(s: QuadValueId, p: QuadValueId, o: QuadValueId): Long?

    /**
     * Stores a Quad if it doesn't already exist and returns its id
     */
    fun addQuad(quad: Quad): Long {

        val s = getOrAddQuadValueId(quad.subject)
        val p = getOrAddQuadValueId(quad.predicate)
        val o = getOrAddQuadValueId(quad.`object`)

        val existingId = getQuadId(s, p, o)

        if (existingId != null) {
            return existingId
        }

        return insert(s, p, o, quad.id.toString())

    }

    protected fun getQuadId(quad: Quad): Long? {
        val s = getQuadValueId(quad.subject)

        if (s.first == null || s.second == null) {
            return null
        }

        val p = getQuadValueId(quad.predicate)

        if (p.first == null || p.second == null) {
            return null
        }

        val o = getQuadValueId(quad.`object`)

        if (o.first == null || o.second == null) {
            return null
        }

        return getQuadId(s.first!! to s.second!!, p.first!! to p.second!!, o.first!! to o.second!!)
    }


    override fun contains(element: Quad): Boolean = getQuadId(element) != null

    override fun containsAll(elements: Collection<Quad>): Boolean {
        return elements.all { contains(it) }
    }

    protected fun quadHash(sType: Int, s: Long, pType: Int, p: Long, oType: Int, o: Long): String {

        val buf = ByteBuffer.wrap(ByteArray(36))
        buf.putInt(sType)
        buf.putLong(s)
        buf.putInt(pType)
        buf.putLong(p)
        buf.putInt(oType)
        buf.putLong(o)
        return buf.array().toBase64()
    }

    override fun add(element: Quad): Boolean {

        val s = getOrAddQuadValueId(element.subject)
        val p = getOrAddQuadValueId(element.predicate)
        val o = getOrAddQuadValueId(element.`object`)

        val existingId = getQuadId(s, p, o)

        if (existingId != null) {
            return false
        }

        insert(s, p, o, element.id.toString())

        return true
    }

    /**
     * Materialize a composed FilterDescriptor into a single result.
     * Subclasses override to generate optimized SQL.
     * Default replays filter calls sequentially (same as current behavior).
     */
    open fun materializeFilter(descriptor: FilterDescriptor): QuadSet {
        var result: QuadSet = this as QuadSet

        if (descriptor.subjects != null || descriptor.predicates != null || descriptor.objects != null) {
            result = result.filter(
                descriptor.subjects?.toList(),
                descriptor.predicates?.toList(),
                descriptor.objects?.toList()
            )
        }

        for (range in descriptor.rangeFilters) {
            result = result.filterRange(range.predicate, range.min, range.max)
        }

        for (tf in descriptor.textFilters) {
            result = result.textFilter(tf.predicate, tf.searchText)
        }

        for (ef in descriptor.exclusionFilters) {
            result = result.filterNotIn(ef.predicate, ef.excludedValues)
        }

        if (descriptor.orderBy.isNotEmpty() || descriptor.limit < Int.MAX_VALUE || descriptor.offset > 0) {
            result = result.orderedFilter(
                null, null, null,
                descriptor.orderBy,
                descriptor.limit,
                descriptor.offset
            )
        }

        return result
    }

    /**
     * Estimate the number of quads matching the given predicate.
     * Returns null if no estimate is available (caller should use heuristics).
     * Subclasses override to provide accurate estimates (e.g., from pg_stats).
     */
    open fun estimatePredicateCardinality(predicate: QuadValue): Long? = null

    open fun dump(writer: Writer, chunkSize: Int) {
        TODO("Not yet implemented")
    }
}