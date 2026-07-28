package org.megras.graphstore.db

import com.google.common.cache.CacheBuilder
import com.pgvector.PGvector

import org.megras.data.graph.*
import org.megras.graphstore.BasicQuadSet
import org.megras.graphstore.Distance
import org.megras.graphstore.MutableQuadSet
import org.megras.graphstore.OrderSpec
import org.megras.graphstore.QuadComponent
import org.megras.graphstore.QuadSet
import org.megras.graphstore.db.dict.QuadValueDictionary
import org.megras.graphstore.deferred.FilterDescriptor
import org.megras.id.SemanticId
import org.megras.util.TimingConfig
import org.slf4j.LoggerFactory
import java.io.Writer
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.Expression
import org.jetbrains.exposed.v1.core.IColumnType
import org.jetbrains.exposed.v1.core.JoinType
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.Schema
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.inList
import org.jetbrains.exposed.v1.core.or
import org.jetbrains.exposed.v1.core.stringLiteral
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.insertReturning
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction


class PostgresStore(
    private val dictionary: QuadValueDictionary,
    host: String = "localhost:5432/megras",
    user: String = "megras",
    password: String = "megras"
) : AbstractDbStore() {

    private val logger = LoggerFactory.getLogger(this.javaClass)
    private val TIMING_ENABLED get() = TimingConfig.enabled

    companion object {
        // Threshold for switching from OR-chain to VALUES clause approach
        // OR chains with more than this many conditions become very slow in PostgreSQL
        private const val OR_CHAIN_THRESHOLD = 100

        // Chunk size for cursor-based iteration and dump
        private const val CURSOR_CHUNK_SIZE = 10_000
    }

    object QuadsTable : Table("quads") {
        val id: Column<Long> = long("id").autoIncrement().uniqueIndex()
        val sType: Column<Int> = integer("s_type").index()
        val s: Column<Long> = long("s").index()
        val pType: Column<Int> = integer("p_type").index()
        val p: Column<Long> = long("p").index()
        val oType: Column<Int> = integer("o_type").index()
        val o: Column<Long> = long("o").index()
        val hash: Column<String> = varchar("hash", 48).uniqueIndex()
        val semid: Column<String> = varchar("semid", 64).uniqueIndex()

        override val primaryKey = PrimaryKey(id)

        val sIndex = index(false, sType, s)
        val pIndex = index(false, p, pType)
        val oIndex = index(false, oType, o)

        val spIndex = index(false, sType, s, pType, p)
        val poIndex = index(false, pType, p, oType, o)

    }

    object VectorTypesTable : Table("vector_types") {
        val id: Column<Int> = integer("id").autoIncrement().uniqueIndex()
        val type: Column<Int> = integer("type")
        val length: Column<Int> = integer("length")

        override val primaryKey = PrimaryKey(id)
    }

    private val db: Database

    init {
        val config = HikariConfig().apply {
            jdbcUrl = "jdbc:postgresql://$host"
            driverClassName = "org.postgresql.Driver"
            username = user
            this.password = password

            // Optimization settings for high-speed transactions
            maximumPoolSize = 10
            isAutoCommit = false // Exposed manages transactions, so disable auto-commit
            transactionIsolation = "TRANSACTION_REPEATABLE_READ"

            // Value is in milliseconds (28 * 60 * 1000 = 1,680,000ms)
            maxLifetime = 1680000

            // Also recommend setting an idle timeout, so unused connections don't waste resources
            // Retire connections idle for more than 10 minutes (10 * 60 * 1000 = 600,000ms)
            idleTimeout = 600000
        }

        // 2. Create the Connection Pool
        val dataSource = HikariDataSource(config)

        // 3. Connect Exposed using the Pool
        db = Database.connect(dataSource)

        transaction {
            val schema = Schema("megras")
            SchemaUtils.createSchema(schema)
            SchemaUtils.setSchema(schema)
        }
    }

    override fun setup() {
        dictionary.setup()
        transaction {
            SchemaUtils.create(QuadsTable, VectorTypesTable)
            exec("CREATE EXTENSION IF NOT EXISTS vector;")
        }
        // Ensure HNSW indexes exist on all vector tables for fast KNN queries
        ensureVectorIndexes()
    }

    override fun lookUpDoubleValueIds(doubleValues: Set<DoubleValue>): Map<DoubleValue, QuadValueId> =
        dictionary.lookUpDoubleValueIds(doubleValues)

    override fun lookUpStringValueIds(stringValues: Set<StringValue>): Map<StringValue, QuadValueId> =
        dictionary.lookUpStringValueIds(stringValues)

    override fun lookUpPrefixIds(prefixValues: Set<String>): Map<String, Int> =
        dictionary.lookUpPrefixIds(prefixValues)

    override fun lookUpSuffixIds(suffixValues: Set<String>): Map<String, Long> =
        dictionary.lookUpSuffixIds(suffixValues)

    override fun lookUpVectorValueIds(vectorValues: Set<VectorValue>): Map<VectorValue, QuadValueId> {
            if (vectorValues.isEmpty()) {
                return emptyMap()
            }

            val returnMap = HashMap<VectorValue, QuadValueId>(vectorValues.size)

            vectorValues.groupBy { it.type to it.length }.forEach { (properties, vectorList) ->

                val vectorsInGroup = vectorList.toSet() // Set<VectorValue>
                val vectorTable = getOrCreateVectorTable(properties.first, properties.second)

                val queryResults = mutableListOf<Pair<Long, VectorValue>>()

                transaction {
                    when (properties.first) {
                        VectorValue.Type.Float -> {
                            @Suppress("UNCHECKED_CAST")
                            val specificValueColumn = vectorTable.value as Column<FloatVectorValue>
                            val floatVectors = vectorsInGroup.filterIsInstance<FloatVectorValue>()
                            if (floatVectors.isNotEmpty()) {
                                floatVectors.chunked(100).forEach { chunk ->
                                    queryResults.addAll(
                                        vectorTable.selectAll().where { specificValueColumn inList chunk }
                                            .map { it[vectorTable.id] to it[specificValueColumn] }
                                    )
                                }
                            }
                        }
                        VectorValue.Type.Double -> {
                            @Suppress("UNCHECKED_CAST")
                            val specificValueColumn = vectorTable.value as Column<DoubleVectorValue>
                            val doubleVectors = vectorsInGroup.filterIsInstance<DoubleVectorValue>()
                            if (doubleVectors.isNotEmpty()) {
                                doubleVectors.chunked(10000).forEach { chunk ->
                                    queryResults.addAll(
                                        vectorTable.selectAll().where { specificValueColumn inList chunk }
                                            .map { it[vectorTable.id] to it[specificValueColumn] }
                                    )
                                }
                            }
                        }
                        VectorValue.Type.Long -> {
                            @Suppress("UNCHECKED_CAST")
                            val specificValueColumn = vectorTable.value as Column<LongVectorValue>
                            val longVectors = vectorsInGroup.filterIsInstance<LongVectorValue>()
                            if (longVectors.isNotEmpty()) {
                                longVectors.chunked(10000).forEach { chunk ->
                                    queryResults.addAll(
                                        vectorTable.selectAll().where { specificValueColumn inList chunk }
                                            .map { it[vectorTable.id] to it[specificValueColumn] }
                                    )
                                }
                            }
                        }
                    }
                }

                for ((id, value) in queryResults) {
                    val quadValueId = (-vectorTable.typeId + VECTOR_ID_OFFSET) to id
                    returnMap[value] = quadValueId
                }
            }

            return returnMap
        }

    override fun insertDoubleValues(doubleValues: Set<DoubleValue>): Map<DoubleValue, QuadValueId> =
        dictionary.insertDoubleValues(doubleValues)

    override fun insertStringValues(stringValues: Set<StringValue>): Map<StringValue, QuadValueId> =
        dictionary.insertStringValues(stringValues)

    override fun insertPrefixValues(prefixValues: Set<String>): Map<String, Int> =
        dictionary.insertPrefixValues(prefixValues)

    override fun insertSuffixValues(suffixValues: Set<String>): Map<String, Long> =
        dictionary.insertSuffixValues(suffixValues)

override fun insertVectorValueIds(vectorValues: Set<VectorValue>): Map<VectorValue, QuadValueId> {
    val maps = vectorValues.groupBy { it.type to it.length }.map { (properties, v) ->
        val vectorsInGroup = v // v is List<VectorValue>
        val currentVectorType = properties.first
        val currentLength = properties.second
        // getOrCreateVectorTable returns a VectorTable instance where 'value' is Column<out VectorValue>
        // but its actual underlying type corresponds to currentVectorType
        val vectorTable = getOrCreateVectorTable(currentVectorType, currentLength)

        if (vectorsInGroup.isEmpty()) {
            return emptyMap()
        }
        transaction {
            vectorsInGroup.map { vec ->

                val id = vectorTable.insertReturning {
                    when (currentVectorType) {
                        VectorValue.Type.Float -> {
                            @Suppress("UNCHECKED_CAST")
                            it[vectorTable.value as Column<FloatVectorValue>] = vec as FloatVectorValue
                        }

                        VectorValue.Type.Double -> {
                            @Suppress("UNCHECKED_CAST")
                            it[vectorTable.value as Column<DoubleVectorValue>] = vec as DoubleVectorValue
                        }

                        VectorValue.Type.Long -> {
                            @Suppress("UNCHECKED_CAST")
                            it[vectorTable.value as Column<LongVectorValue>] = vec as LongVectorValue
                        }
                    }
                }.single()[vectorTable.id]

                val qvid = QuadValueId(-vectorTable.typeId + VECTOR_ID_OFFSET, id)

                vec to qvid

            }
        }
    }

    return maps.flatten().toMap()

}

    class VectorTable(val typeId: Int, type: VectorValue.Type, length: Int) : Table("vector_values_$typeId") {
        val id: Column<Long> = long("id").autoIncrement().uniqueIndex()
        val value = when (type) {
            VectorValue.Type.Float -> registerColumn<FloatVectorValue>("value",
                object : IColumnType<FloatVectorValue> {
                    override var nullable = false

                    override fun sqlType(): String = "vector($length)"
                    override fun valueFromDB(value: Any): FloatVectorValue {
                        return FloatVectorValue.parse(value.toString())
                    }

                    override fun notNullValueToDB(value: FloatVectorValue): Any {
                        return PGvector((value as FloatVectorValue).vector)
                    }
                }
            )
            VectorValue.Type.Double -> registerColumn<DoubleVectorValue>("value",
                object : IColumnType<DoubleVectorValue> {
                    override var nullable = false
                    override fun sqlType(): String = "vector($length)"
                    override fun valueFromDB(value: Any): DoubleVectorValue {
                        return DoubleVectorValue.parse(value.toString())
                    }

                    override fun notNullValueToDB(value: DoubleVectorValue): Any {
                        return PGvector(value.toString().replace("^^DoubleVector", ""))
                    }
                }
            )
            VectorValue.Type.Long -> registerColumn<LongVectorValue>("value",
                object : IColumnType<LongVectorValue> {
                    override var nullable = false
                    override fun sqlType(): String = "vector($length)"
                    override fun valueFromDB(value: Any): LongVectorValue {
                        return LongVectorValue.parse(value.toString())
                    }

                    override fun notNullValueToDB(value: LongVectorValue): Any {
                        return PGvector(value.toString().replace("^^LongVector", ""))
                    }
                }
            )
        }

        override val primaryKey = PrimaryKey(this.id)
    }

    private fun getOrCreateVectorTable(type: VectorValue.Type, length: Int): VectorTable {
        fun createTable(): VectorTable {
            val result = transaction {
                VectorTypesTable.insert {
                    it[VectorTypesTable.type] = type.ordinal
                    it[VectorTypesTable.length] = length
                }
            }
            val id = result[VectorTypesTable.id]
            val vectorTable = VectorTable(id, type, length)
            transaction {
                SchemaUtils.create(vectorTable)
                // Create HNSW index for fast cosine distance KNN queries
                // This dramatically improves nearestNeighbor performance from O(n) to O(log n)
                exec("CREATE INDEX IF NOT EXISTS vector_values_${id}_hnsw_idx ON megras.vector_values_${id} USING hnsw (value vector_cosine_ops) WITH (m = 16, ef_construction = 64)")
            }

            return vectorTable
        }

        return getVectorTable(type, length) ?: createTable()
    }

    /**
     * Ensures HNSW indexes exist on all vector tables for fast KNN queries.
     * Call this once during setup or manually to add indexes to existing tables.
     */
    fun ensureVectorIndexes() {
        val startTime = System.currentTimeMillis()

        transaction {
            val vectorTypes = VectorTypesTable.selectAll().map {
                Triple(it[VectorTypesTable.id], it[VectorTypesTable.type], it[VectorTypesTable.length])
            }

            for ((id, _, _) in vectorTypes) {
                try {
                    exec("CREATE INDEX IF NOT EXISTS vector_values_${id}_hnsw_idx ON megras.vector_values_${id} USING hnsw (value vector_cosine_ops) WITH (m = 16, ef_construction = 64)")
                } catch (e: Exception) {
                    logger.warn("Failed to create HNSW index for vector_values_$id: ${e.message}")
                }
            }
        }

        if (TIMING_ENABLED) logger.info("Vector index check completed in ${System.currentTimeMillis() - startTime}ms")
    }

    private fun getVectorTable(type: VectorValue.Type, length: Int): VectorTable? {
        val tableId = transaction {
            VectorTypesTable.selectAll().where {
                (VectorTypesTable.type eq type.ordinal) and (VectorTypesTable.length eq length)
            }.map { it[VectorTypesTable.id] }.firstOrNull()
        }

        return if (tableId != null) {
            VectorTable(tableId, type, length)
        } else {
            null
        }
    }

    override fun lookUpDoubleValues(ids: Set<Long>): Map<QuadValueId, DoubleValue> =
        dictionary.lookUpDoubleValues(ids)

    override fun lookUpStringValues(ids: Set<Long>): Map<QuadValueId, StringValue> =
        dictionary.lookUpStringValues(ids)

    override fun lookUpVectorValues(ids: Set<QuadValueId>): Map<QuadValueId, VectorValue> {
        if (ids.isEmpty()) {
            return emptyMap()
        }

        val returnMap = HashMap<QuadValueId, VectorValue>(ids.size)

        ids.groupBy { it.first }.forEach { (type, quadValueIds) ->
            val longIds = quadValueIds.map { it.second }
            longIds.chunked(10000).forEach { chunk ->
                val values = getVectorQuadValues(type, chunk)
                values.forEach { (longId, vectorValue) ->
                    returnMap[type to longId] = vectorValue
                }
            }
        }
        //TODO batch by type
        return returnMap
    }

    private fun getVectorQuadValues(type: Int, ids: List<Long>): Map<Long, VectorValue> {
        if (ids.isEmpty()) return emptyMap()

        val internalId = -type + VECTOR_ID_OFFSET

        val properties = getVectorProperties(internalId) ?: return emptyMap()

        val vectorTable = getOrCreateVectorTable(properties.second, properties.first)

        val result = mutableMapOf<Long, Any>()
        ids.chunked(10000).forEach { chunk ->
            transaction {
                vectorTable.selectAll().where {
                    vectorTable.id inList chunk
                }.forEach {
                    result[it[vectorTable.id]] = it[vectorTable.value]
                }
            }
        }

        val map = mutableMapOf<Long, VectorValue>()

        when(properties.second) {
            VectorValue.Type.Double -> {
                result.forEach { (id, value) ->
                    map[id] = value as DoubleVectorValue
                }
            }
            VectorValue.Type.Long -> {
                result.forEach { (id, value) ->
                    map[id] = value as LongVectorValue
                }
            }
            VectorValue.Type.Float -> {
                result.forEach { (id, value) ->
                    map[id] = value as FloatVectorValue
                }
            }
        }

        return map
    }

    private fun getVectorProperties(type: Int): Pair<Int, VectorValue.Type>? {
        return transaction {
            VectorTypesTable.selectAll()
                .where { VectorTypesTable.id eq type }
                .firstOrNull()
                ?.let { row ->
                    row[VectorTypesTable.length] to VectorValue.Type.values()[row[VectorTypesTable.type]]
                }
        }
    }

    override fun lookUpPrefixes(ids: Set<Int>): Map<Int, String> =
        dictionary.lookUpPrefixes(ids)

    override fun lookUpSuffixes(ids: Set<Long>): Map<Long, String> =
        dictionary.lookUpSuffixes(ids)

    override fun insert(s: QuadValueId, p: QuadValueId, o: QuadValueId, semidStr: String): Long {
        return transaction {
            QuadsTable.insert {
                it[sType] = s.first
                it[this.s] = s.second
                it[pType] = p.first
                it[this.p] = p.second
                it[oType] = o.first
                it[this.o] = o.second
                it[hash] = quadHash(s.first, s.second, p.first, p.second, o.first, o.second)
                it[semid] = semidStr
            }[QuadsTable.id]
        }
    }

    override fun getQuadId(s: QuadValueId, p: QuadValueId, o: QuadValueId): Long? {
        return transaction {
            QuadsTable.select(QuadsTable.id).where {
                QuadsTable.hash eq quadHash(s.first, s.second, p.first, p.second, o.first, o.second)
            }.firstOrNull()?.get(QuadsTable.id)
        }
    }

    override fun getId(id: SemanticId): Quad? {
        val semidStr = id.toString()
        val tup = transaction {
            QuadsTable.select(QuadsTable.sType, QuadsTable.s, QuadsTable.pType, QuadsTable.p, QuadsTable.oType, QuadsTable.o)
                .where { QuadsTable.semid eq semidStr }
                .firstOrNull()
        } ?: return null
        val sv = tup[QuadsTable.sType] to tup[QuadsTable.s]
        val pv = tup[QuadsTable.pType] to tup[QuadsTable.p]
        val ov = tup[QuadsTable.oType] to tup[QuadsTable.o]
        val values = getQuadValues(listOf(sv, pv, ov))
        val s = values[sv] ?: return null
        val p = values[pv] ?: return null
        val o = values[ov] ?: return null
        return Quad(s, p, o)
    }

    private val idCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build<Long, Triple<QuadValueId, QuadValueId, QuadValueId>>()

    private val predicateCardinalityCache = CacheBuilder.newBuilder()
        .maximumSize(1000L)
        .expireAfterWrite(5, java.util.concurrent.TimeUnit.MINUTES)
        .build<QuadValueId, Long>()

    private fun getIds(ids: Collection<Long>): QuadSet {
        if (ids.isEmpty()) {
            return BasicQuadSet()
        }

        val mutableIds = ids.toMutableSet()

        val quadIds = mutableSetOf<Pair<Long, Triple<QuadValueId, QuadValueId, QuadValueId>>>()

        mutableIds.removeIf {
            val cached = idCache.getIfPresent(it)
            if (cached != null) {
                quadIds.add(it to cached)
                true
            } else {
                false
            }
        }

        if (mutableIds.isNotEmpty()) {
            //TODO: optimize to custom SQL
            mutableIds.chunked(10000).forEach { chunk ->
                transaction {
                    val lookUpQuadIds = QuadsTable.select(QuadsTable.id, QuadsTable.sType, QuadsTable.s, QuadsTable.pType, QuadsTable.p, QuadsTable.oType, QuadsTable.o)
                        .where { QuadsTable.id inList chunk }.map {
                            it[QuadsTable.id] to Triple(
                                (it[QuadsTable.sType] to it[QuadsTable.s]),
                                (it[QuadsTable.pType] to it[QuadsTable.p]),
                                (it[QuadsTable.oType] to it[QuadsTable.o]),
                            )
                        }

                    lookUpQuadIds.forEach {
                        idCache.put(it.first, it.second)
                    }

                    quadIds.addAll(lookUpQuadIds)
                }
            }
        }

        return getIds(quadIds)
    }

    fun getIds(quadIds: Set<Pair<Long, Triple<QuadValueId, QuadValueId, QuadValueId>>>): QuadSet {
        val quadValueIds = quadIds.flatMap { listOf(it.second.first, it.second.second, it.second.third) }.toSet()
        val quadValues = getQuadValues(quadValueIds)

        return BasicQuadSet(
            quadIds.mapNotNull {
                val s = quadValues[it.second.first]
                val p = quadValues[it.second.second]
                val o = quadValues[it.second.third]

                if (s != null && p != null && o != null) {
                    Quad(s, p, o)
                } else {
                    null
                }
            }.toSet()
        )
    }

    override fun filterSubject(subject: QuadValue): QuadSet {
        val id = getQuadValueId(subject)

        if (id.first == null || id.second == null) {
            return BasicQuadSet()
        }

        val quadIds = transaction {
            QuadsTable.select(QuadsTable.id).where { (QuadsTable.sType eq id.first!!) and (QuadsTable.s eq id.second!!) }
                .map { it[QuadsTable.id] }
        }

        return getIds(quadIds)
    }

    override fun filterPredicate(predicate: QuadValue): QuadSet {
        val id = getQuadValueId(predicate)

        if (id.first == null || id.second == null) {
            return BasicQuadSet()
        }

        val quadIds = transaction {
            QuadsTable.select(QuadsTable.id).where { (QuadsTable.pType eq id.first!!) and (QuadsTable.p eq id.second!!) }
                .map { it[QuadsTable.id] }
        }

        return getIds(quadIds)
    }

    override fun filterObject(`object`: QuadValue): QuadSet {
        val id = getQuadValueId(`object`)

        if (id.first == null || id.second == null) {
            return BasicQuadSet()
        }

        val quadIds = transaction {
            QuadsTable.select(QuadsTable.id).where { (QuadsTable.oType eq id.first!!) and (QuadsTable.o eq id.second!!) }
                .map { it[QuadsTable.id] }
        }

        return getIds(quadIds)
    }

    private fun getIdsForFilter(ids: List<Pair<Int, Long>>?, part: Char): Set<Long>? {
        if (ids == null) return null
        if (ids.isEmpty()) return emptySet()

        val (typeColumn, idColumn) = when (part) {
            's' -> QuadsTable.sType to QuadsTable.s
            'p' -> QuadsTable.pType to QuadsTable.p
            'o' -> QuadsTable.oType to QuadsTable.o
            else -> throw IllegalArgumentException("part must be 's', 'p', or 'o'")
        }

        val quadIds = mutableSetOf<Long>()
        // Each pair in the chunk results in two parameters
        ids.chunked(10000).forEach { chunk ->
            transaction {
                val filter = chunk.map { (typeColumn eq it.first) and (idColumn eq it.second) }.reduce { acc, op -> acc or op }
                QuadsTable.select(QuadsTable.id).where(filter).mapTo(quadIds) { it[QuadsTable.id] }
            }
        }
        return quadIds
    }

    override fun filter(
        subjects: Collection<QuadValue>?,
        predicates: Collection<QuadValue>?,
        objects: Collection<QuadValue>?
    ): QuadSet {

        val startTotal = if (TIMING_ENABLED) System.currentTimeMillis() else 0L

        // 1. Handle Trivial Cases (empty or no filters)
        val start1 = if (TIMING_ENABLED) System.currentTimeMillis() else 0L
        if (subjects == null && predicates == null && objects == null) return this
        if (subjects?.isEmpty() == true || predicates?.isEmpty() == true || objects?.isEmpty() == true) return BasicQuadSet()
        if (TIMING_ENABLED) logger.info("Time spent in Filter Trivial Checks: ${System.currentTimeMillis() - start1}ms")


        // 2. Resolve QuadValues to (typeId, longId) pairs
        val start2 = if (TIMING_ENABLED) System.currentTimeMillis() else 0L
        val allFilterValues = (subjects?.toSet() ?: emptySet()) + (predicates?.toSet() ?: emptySet()) + (objects?.toSet() ?: emptySet())
        val filterIds = getOrAddQuadValueIds(allFilterValues, false)

        val subjectFilterIds = subjects?.mapNotNull { filterIds[it] }
        val predicateFilterIds = predicates?.mapNotNull { filterIds[it] }
        val objectFilterIds = objects?.mapNotNull { filterIds[it] }

        if ((subjects != null && subjectFilterIds!!.isEmpty()) ||
            (predicates != null && predicateFilterIds!!.isEmpty()) ||
            (objects != null && objectFilterIds!!.isEmpty())) {
            if (TIMING_ENABLED) logger.info("Time spent in Filter ID Resolution (Early Exit): ${System.currentTimeMillis() - start2}ms")
            return BasicQuadSet()
        }
        if (TIMING_ENABLED) logger.info("Time spent in Filter ID Resolution (getOrAddQuadValueIds): ${System.currentTimeMillis() - start2}ms")

        // Check if any filter set is large enough to warrant VALUES clause approach
        val useLargeSubjectFilter = subjectFilterIds != null && subjectFilterIds.size > OR_CHAIN_THRESHOLD
        val useLargePredicateFilter = predicateFilterIds != null && predicateFilterIds.size > OR_CHAIN_THRESHOLD
        val useLargeObjectFilter = objectFilterIds != null && objectFilterIds.size > OR_CHAIN_THRESHOLD

        val finalSeptuples = mutableSetOf<Pair<Long, Triple<QuadValueId, QuadValueId, QuadValueId>>>()

        // 3. Execute Database Query - use optimized approach for large filter sets
        val start3 = if (TIMING_ENABLED) System.currentTimeMillis() else 0L

        if (useLargeSubjectFilter || useLargePredicateFilter || useLargeObjectFilter) {
            // Use VALUES clause approach for large filter sets
            if (TIMING_ENABLED) {
                logger.info("Using VALUES clause approach - subjects: ${subjectFilterIds?.size ?: "null"}, predicates: ${predicateFilterIds?.size ?: "null"}, objects: ${objectFilterIds?.size ?: "null"}")
            }
            executeFilterWithValuesClause(
                subjectFilterIds, predicateFilterIds, objectFilterIds,
                useLargeSubjectFilter, useLargePredicateFilter, useLargeObjectFilter,
                finalSeptuples
            )
        } else {
            // Use traditional OR-chain approach for small filter sets
            executeFilterWithOrChain(subjectFilterIds, predicateFilterIds, objectFilterIds, finalSeptuples)
        }

        if (TIMING_ENABLED) logger.info("Time spent in DB Transaction (Main Query Execution): ${System.currentTimeMillis() - start3}ms")

        // 4. Return the resulting QuadSet
        val start4 = if (TIMING_ENABLED) System.currentTimeMillis() else 0L
        val quadSet = getIds(finalSeptuples) // Converting IDs back to QuadValues
        if (TIMING_ENABLED) logger.info("Time spent in Final ID Conversion (getIds): ${System.currentTimeMillis() - start4}ms")

        if (TIMING_ENABLED) logger.info("Total time spent in PostgresStore.filter: ${System.currentTimeMillis() - startTotal}ms")
        return quadSet
    }

    /**
     * Execute filter using traditional OR-chain approach (efficient for small filter sets)
     */
    private fun executeFilterWithOrChain(
        subjectFilterIds: List<Pair<Int, Long>>?,
        predicateFilterIds: List<Pair<Int, Long>>?,
        objectFilterIds: List<Pair<Int, Long>>?,
        finalSeptuples: MutableSet<Pair<Long, Triple<QuadValueId, QuadValueId, QuadValueId>>>
    ) {
        transaction {
            var condition: Op<Boolean> = Op.TRUE

            if (subjectFilterIds != null) {
                val sCondition = subjectFilterIds
                    .map { (QuadsTable.sType eq it.first) and (QuadsTable.s eq it.second) }
                    .reduce { acc, o -> acc or o }
                condition = condition and sCondition
            }
            if (predicateFilterIds != null) {
                val pCondition = predicateFilterIds
                    .map { (QuadsTable.pType eq it.first) and (QuadsTable.p eq it.second) }
                    .reduce { acc, o -> acc or o }
                condition = condition and pCondition
            }
            if (objectFilterIds != null) {
                val oCondition = objectFilterIds
                    .map { (QuadsTable.oType eq it.first) and (QuadsTable.o eq it.second) }
                    .reduce { acc, o -> acc or o }
                condition = condition and oCondition
            }

            val result = QuadsTable.select(
                QuadsTable.id, QuadsTable.sType, QuadsTable.s, QuadsTable.pType, QuadsTable.p, QuadsTable.oType, QuadsTable.o
            ).where(condition)
                .map {
                    it[QuadsTable.id] to Triple(
                        (it[QuadsTable.sType] to it[QuadsTable.s]),
                        (it[QuadsTable.pType] to it[QuadsTable.p]),
                        (it[QuadsTable.oType] to it[QuadsTable.o]),
                    )
                }

            finalSeptuples.addAll(result)
        }
    }

    /**
     * Execute filter using VALUES clause approach (efficient for large filter sets)
     * Uses raw SQL with VALUES clause which PostgreSQL optimizes much better than large OR chains
     */
    private fun executeFilterWithValuesClause(
        subjectFilterIds: List<Pair<Int, Long>>?,
        predicateFilterIds: List<Pair<Int, Long>>?,
        objectFilterIds: List<Pair<Int, Long>>?,
        useLargeSubjectFilter: Boolean,
        useLargePredicateFilter: Boolean,
        useLargeObjectFilter: Boolean,
        finalSeptuples: MutableSet<Pair<Long, Triple<QuadValueId, QuadValueId, QuadValueId>>>
    ) {
        transaction {
            // Build the SQL query with VALUES clauses for large filters
            val sqlBuilder = StringBuilder()
            sqlBuilder.append("SELECT q.id, q.s_type, q.s, q.p_type, q.p, q.o_type, q.o FROM quads q")

            val joins = mutableListOf<String>()
            val conditions = mutableListOf<String>()

            // Handle subject filter
            if (subjectFilterIds != null) {
                if (useLargeSubjectFilter) {
                    val valuesClause = subjectFilterIds.joinToString(",") { "(${it.first},${it.second})" }
                    joins.add(" INNER JOIN (VALUES $valuesClause) AS sf(s_type, s_id) ON q.s_type = sf.s_type AND q.s = sf.s_id")
                } else {
                    val orConditions = subjectFilterIds.joinToString(" OR ") { "(q.s_type = ${it.first} AND q.s = ${it.second})" }
                    conditions.add("($orConditions)")
                }
            }

            // Handle predicate filter
            if (predicateFilterIds != null) {
                if (useLargePredicateFilter) {
                    val valuesClause = predicateFilterIds.joinToString(",") { "(${it.first},${it.second})" }
                    joins.add(" INNER JOIN (VALUES $valuesClause) AS pf(p_type, p_id) ON q.p_type = pf.p_type AND q.p = pf.p_id")
                } else {
                    val orConditions = predicateFilterIds.joinToString(" OR ") { "(q.p_type = ${it.first} AND q.p = ${it.second})" }
                    conditions.add("($orConditions)")
                }
            }

            // Handle object filter
            if (objectFilterIds != null) {
                if (useLargeObjectFilter) {
                    val valuesClause = objectFilterIds.joinToString(",") { "(${it.first},${it.second})" }
                    joins.add(" INNER JOIN (VALUES $valuesClause) AS of(o_type, o_id) ON q.o_type = of.o_type AND q.o = of.o_id")
                } else {
                    val orConditions = objectFilterIds.joinToString(" OR ") { "(q.o_type = ${it.first} AND q.o = ${it.second})" }
                    conditions.add("($orConditions)")
                }
            }

            // Build final query
            for (join in joins) {
                sqlBuilder.append(join)
            }

            if (conditions.isNotEmpty()) {
                sqlBuilder.append(" WHERE ")
                sqlBuilder.append(conditions.joinToString(" AND "))
            }

            val sql = sqlBuilder.toString()

            // Execute raw SQL
            exec(sql) { rs ->
                while (rs.next()) {
                    val id = rs.getLong("id")
                    val sType = rs.getInt("s_type")
                    val s = rs.getLong("s")
                    val pType = rs.getInt("p_type")
                    val p = rs.getLong("p")
                    val oType = rs.getInt("o_type")
                    val o = rs.getLong("o")

                    finalSeptuples.add(
                        id to Triple(
                            (sType to s),
                            (pType to p),
                            (oType to o)
                        )
                    )
                }
            }
        }
    }

    override fun toMutable(): MutableQuadSet = this

    override fun toSet(): Set<Quad> {
        TODO("Not yet implemented")
    }

    override fun plus(other: QuadSet): QuadSet {
        TODO("Not yet implemented")
    }

    private class VectorDistance(
        private val column: Expression<*>,
        private val target: VectorValue,
        private val distance: Distance
    ) : Op<Float>() {
        override fun toQueryBuilder(queryBuilder: QueryBuilder) {
            val op = when (distance) {
                Distance.COSINE -> "<=>"
                Distance.DOTPRODUCT -> "<#>"
            }

            // The PGvector class from the library handles the string representation.
            val pgVector = when(target) {
                is FloatVectorValue -> PGvector(target.vector)
                // pgvector works with float vectors.
                is DoubleVectorValue -> PGvector(target.vector.map { it.toFloat() }.toFloatArray())
                is LongVectorValue -> PGvector(target.vector.map { it.toFloat() }.toFloatArray())
                else -> {TODO("Unsupported vector type: ${target.type}") }
            }

            queryBuilder.append("(")
            column.toQueryBuilder(queryBuilder)
            queryBuilder.append(" $op ")
            queryBuilder.append(stringLiteral(pgVector.toString()))
            queryBuilder.append(")")
        }
    }

    override fun nearestNeighbor(predicate: QuadValue, `object`: VectorValue, count: Int, distance: Distance, invert: Boolean): QuadSet {
        val startTime = if (TIMING_ENABLED) System.currentTimeMillis() else 0L

        val predicateId = getQuadValueId(predicate)
        if (predicateId.first == null || predicateId.second == null) {
            if (TIMING_ENABLED) logger.info("PostgresStore.nearestNeighbor: Unknown predicate, returning empty in ${System.currentTimeMillis() - startTime}ms")
            return BasicQuadSet()
        }

        // We are querying, so we only care about existing tables.
        val vectorTable = getVectorTable(`object`.type, `object`.length)
        if (vectorTable == null) {
            if (TIMING_ENABLED) logger.info("PostgresStore.nearestNeighbor: No vector table found for type=${`object`.type}, length=${`object`.length}, returning empty in ${System.currentTimeMillis() - startTime}ms")
            return BasicQuadSet()
        }

        val distanceExpression = VectorDistance(vectorTable.value, `object`, distance)

        val queryStartTime = if (TIMING_ENABLED) System.currentTimeMillis() else 0L
        val quadIds = transaction {
            QuadsTable.join(
                vectorTable,
                JoinType.INNER,
                onColumn = QuadsTable.o,
                otherColumn = vectorTable.id
            ) {
                (QuadsTable.oType eq (-vectorTable.typeId + VECTOR_ID_OFFSET))
            }
            .select(QuadsTable.id)
            .where {
                (QuadsTable.pType eq predicateId.first!!) and (QuadsTable.p eq predicateId.second!!)
            }
            .orderBy(distanceExpression to if (invert) SortOrder.DESC else SortOrder.ASC)
            .limit(count)
            .map { it[QuadsTable.id] }
        }
        if (TIMING_ENABLED) logger.info("PostgresStore.nearestNeighbor: DB query returned ${quadIds.size} quad IDs in ${System.currentTimeMillis() - queryStartTime}ms")

        val result = getIds(quadIds)
        if (TIMING_ENABLED) logger.info("PostgresStore.nearestNeighbor: Total time ${System.currentTimeMillis() - startTime}ms, returning ${result.size} quads")
        return result
    }

    override fun exists(subject: QuadValue, predicate: QuadValue): Boolean {
        val startTime = if (TIMING_ENABLED) System.currentTimeMillis() else 0L

        val sId = getQuadValueId(subject)
        if (sId.first == null || sId.second == null) {
            if (TIMING_ENABLED) logger.info("PostgresStore.exists: Unknown subject, returning false in ${System.currentTimeMillis() - startTime}ms")
            return false
        }
        val pId = getQuadValueId(predicate)
        if (pId.first == null || pId.second == null) {
            if (TIMING_ENABLED) logger.info("PostgresStore.exists: Unknown predicate, returning false in ${System.currentTimeMillis() - startTime}ms")
            return false
        }

        val found = transaction {
            var result = false
            exec("SELECT 1 FROM quads WHERE s_type = ${sId.first} AND s = ${sId.second} AND p_type = ${pId.first} AND p = ${pId.second} LIMIT 1") { rs ->
                result = rs.next()
            }
            result
        }

        if (TIMING_ENABLED) logger.info("PostgresStore.exists: Result=$found in ${System.currentTimeMillis() - startTime}ms")
        return found
    }

    override fun filterRange(predicate: QuadValue, min: Double?, max: Double?): QuadSet {
        val startTime = if (TIMING_ENABLED) System.currentTimeMillis() else 0L

        val pId = getQuadValueId(predicate)
        if (pId.first == null || pId.second == null) {
            if (TIMING_ENABLED) logger.info("PostgresStore.filterRange: Unknown predicate, returning empty in ${System.currentTimeMillis() - startTime}ms")
            return BasicQuadSet()
        }

        val conditions = mutableListOf("q.p_type = ${pId.first}", "q.p = ${pId.second}", "q.o_type = ${DOUBLE_LITERAL_TYPE}")

        if (min != null) {
            conditions.add("d.value >= $min")
        }
        if (max != null) {
            conditions.add("d.value <= $max")
        }

        val whereClause = conditions.joinToString(" AND ")
        val sql = """
            SELECT q.id, q.s_type, q.s, q.p_type, q.p, q.o_type, q.o
            FROM quads q
            INNER JOIN literal_double d ON q.o = d.id AND q.o_type = $DOUBLE_LITERAL_TYPE
            WHERE $whereClause
        """.trimIndent()

        val finalSeptuples = mutableSetOf<Pair<Long, Triple<QuadValueId, QuadValueId, QuadValueId>>>()

        transaction {
            exec(sql) { rs ->
                while (rs.next()) {
                    val id = rs.getLong("id")
                    val sType = rs.getInt("s_type")
                    val s = rs.getLong("s")
                    val pType = rs.getInt("p_type")
                    val p = rs.getLong("p")
                    val oType = rs.getInt("o_type")
                    val o = rs.getLong("o")

                    finalSeptuples.add(
                        id to Triple(
                            (sType to s),
                            (pType to p),
                            (oType to o)
                        )
                    )
                }
            }
        }

        val result = getIds(finalSeptuples)
        if (TIMING_ENABLED) logger.info("PostgresStore.filterRange: Returning ${result.size} quads in ${System.currentTimeMillis() - startTime}ms")
        return result
    }

    override fun filterNotIn(predicate: QuadValue, excludedValues: Collection<QuadValue>): QuadSet {
        val startTime = if (TIMING_ENABLED) System.currentTimeMillis() else 0L

        val pId = getQuadValueId(predicate)
        if (pId.first == null || pId.second == null) {
            if (TIMING_ENABLED) logger.info("PostgresStore.filterNotIn: Unknown predicate, returning empty in ${System.currentTimeMillis() - startTime}ms")
            return BasicQuadSet()
        }

        if (excludedValues.isEmpty()) {
            return filterPredicate(predicate)
        }

        // Resolve excluded values to (oType, oId) pairs
        val excludedIds = excludedValues.mapNotNull { getQuadValueId(it) }.filter { it.first != null && it.second != null }

        if (excludedIds.isEmpty()) {
            return filterPredicate(predicate)
        }

        // For large exclusion sets, use NOT IN with VALUES approach via raw SQL
        val finalSeptuples = mutableSetOf<Pair<Long, Triple<QuadValueId, QuadValueId, QuadValueId>>>()

        if (excludedIds.size <= OR_CHAIN_THRESHOLD) {
            // OR-chain approach for small exclusion sets
            transaction {
                val orConditions = excludedIds.joinToString(" OR ") { "(q.o_type = ${it.first} AND q.o = ${it.second})" }
                val sql = """
                    SELECT q.id, q.s_type, q.s, q.p_type, q.p, q.o_type, q.o
                    FROM quads q
                    WHERE q.p_type = ${pId.first} AND q.p = ${pId.second}
                    AND NOT ($orConditions)
                """.trimIndent()

                exec(sql) { rs ->
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val sType = rs.getInt("s_type")
                        val s = rs.getLong("s")
                        val pType = rs.getInt("p_type")
                        val p = rs.getLong("p")
                        val oType = rs.getInt("o_type")
                        val o = rs.getLong("o")

                        finalSeptuples.add(
                            id to Triple(
                                (sType to s),
                                (pType to p),
                                (oType to o)
                            )
                        )
                    }
                }
            }
        } else {
            // VALUES clause approach for large exclusion sets
            transaction {
                val valuesClause = excludedIds.joinToString(",") { "(${it.first},${it.second})" }
                val sql = """
                    SELECT q.id, q.s_type, q.s, q.p_type, q.p, q.o_type, q.o
                    FROM quads q
                    WHERE q.p_type = ${pId.first} AND q.p = ${pId.second}
                    AND NOT EXISTS (
                        SELECT 1 FROM (VALUES $valuesClause) AS ex(o_type, o_id)
                        WHERE q.o_type = ex.o_type AND q.o = ex.o_id
                    )
                """.trimIndent()

                exec(sql) { rs ->
                    while (rs.next()) {
                        val id = rs.getLong("id")
                        val sType = rs.getInt("s_type")
                        val s = rs.getLong("s")
                        val pType = rs.getInt("p_type")
                        val p = rs.getLong("p")
                        val oType = rs.getInt("o_type")
                        val o = rs.getLong("o")

                        finalSeptuples.add(
                            id to Triple(
                                (sType to s),
                                (pType to p),
                                (oType to o)
                            )
                        )
                    }
                }
            }
        }

        val result = getIds(finalSeptuples)
        if (TIMING_ENABLED) logger.info("PostgresStore.filterNotIn: Returning ${result.size} quads in ${System.currentTimeMillis() - startTime}ms")
        return result
    }

    override fun orderedFilter(
        subjects: Collection<QuadValue>?,
        predicates: Collection<QuadValue>?,
        objects: Collection<QuadValue>?,
        orderBy: List<OrderSpec>,
        limit: Int,
        offset: Int
    ): QuadSet {
        // Trivial: no sort keys — delegate to filter() which already has SQL push-down
        if (orderBy.isEmpty() || limit <= 0) return BasicQuadSet()

        val startTime = if (TIMING_ENABLED) System.currentTimeMillis() else 0L

        // 1. Resolve filter values to (typeId, longId) pairs
        val allFilterValues = (subjects?.toSet() ?: emptySet()) +
                (predicates?.toSet() ?: emptySet()) +
                (objects?.toSet() ?: emptySet())
        val filterIds = getOrAddQuadValueIds(allFilterValues, false)

        val subjectFilterIds = subjects?.mapNotNull { filterIds[it] }
        val predicateFilterIds = predicates?.mapNotNull { filterIds[it] }
        val objectFilterIds = objects?.mapNotNull { filterIds[it] }

        if ((subjects != null && subjectFilterIds!!.isEmpty()) ||
            (predicates != null && predicateFilterIds!!.isEmpty()) ||
            (objects != null && objectFilterIds!!.isEmpty())
        ) {
            if (TIMING_ENABLED) logger.info("PostgresStore.orderedFilter: Empty filter, returning empty in ${System.currentTimeMillis() - startTime}ms")
            return BasicQuadSet()
        }

        // 2. Build SQL with LEFT JOINs for sort-by-object
        val sqlBuilder = StringBuilder()
        sqlBuilder.append("SELECT q.id, q.s_type, q.s, q.p_type, q.p, q.o_type, q.o FROM quads q")

        val needsObjectSort = orderBy.any { it.component == QuadComponent.OBJECT }
        if (needsObjectSort) {
            sqlBuilder.append(" LEFT JOIN literal_double ld ON q.o = ld.id AND q.o_type = $DOUBLE_LITERAL_TYPE")
            sqlBuilder.append(" LEFT JOIN literal_string ls ON q.o = ls.id AND q.o_type = $STRING_LITERAL_TYPE")
        }

        // 3. Build WHERE clause using same logic as filter()
        val conditions = mutableListOf<String>()
        if (subjectFilterIds != null) {
            val orChain = subjectFilterIds.joinToString(" OR ") { "(q.s_type = ${it.first} AND q.s = ${it.second})" }
            conditions.add("($orChain)")
        }
        if (predicateFilterIds != null) {
            val orChain = predicateFilterIds.joinToString(" OR ") { "(q.p_type = ${it.first} AND q.p = ${it.second})" }
            conditions.add("($orChain)")
        }
        if (objectFilterIds != null) {
            val orChain = objectFilterIds.joinToString(" OR ") { "(q.o_type = ${it.first} AND q.o = ${it.second})" }
            conditions.add("($orChain)")
        }
        if (conditions.isNotEmpty()) {
            sqlBuilder.append(" WHERE ")
            sqlBuilder.append(conditions.joinToString(" AND "))
        }

        // 4. Build ORDER BY
        val orderClauses = orderBy.map { spec ->
            val sortExpr = when (spec.component) {
                QuadComponent.SUBJECT -> "(q.s_type, q.s)"
                QuadComponent.PREDICATE -> "(q.p_type, q.p)"
                QuadComponent.OBJECT ->
                    "COALESCE(ld.value::TEXT, ls.value, q.o::TEXT)"
            }
            val direction = if (spec.ascending) "ASC" else "DESC"
            "$sortExpr $direction"
        }
        sqlBuilder.append(" ORDER BY ${orderClauses.joinToString(", ")}")

        // 5. LIMIT + OFFSET
        if (limit < Int.MAX_VALUE) {
            sqlBuilder.append(" LIMIT $limit")
        }
        if (offset > 0) {
            sqlBuilder.append(" OFFSET $offset")
        }

        val sql = sqlBuilder.toString()

        // 6. Execute and collect results
        val finalSeptuples = mutableSetOf<Pair<Long, Triple<QuadValueId, QuadValueId, QuadValueId>>>()

        transaction {
            exec(sql) { rs ->
                while (rs.next()) {
                    val id = rs.getLong("id")
                    val sType = rs.getInt("s_type")
                    val s = rs.getLong("s")
                    val pType = rs.getInt("p_type")
                    val p = rs.getLong("p")
                    val oType = rs.getInt("o_type")
                    val o = rs.getLong("o")

                    finalSeptuples.add(
                        id to Triple(
                            (sType to s),
                            (pType to p),
                            (oType to o)
                        )
                    )
                }
            }
        }

        val result = getIds(finalSeptuples)
        if (TIMING_ENABLED) logger.info("PostgresStore.orderedFilter: Returning ${result.size} quads in ${System.currentTimeMillis() - startTime}ms")
        return result
    }


    override fun materializeFilter(descriptor: FilterDescriptor): QuadSet {
        val startTime = if (TIMING_ENABLED) System.currentTimeMillis() else 0L

        if (descriptor.isImpossible) return BasicQuadSet()
        if (descriptor.isTrivial) return this as QuadSet

        // 1. Collect all filter QuadValues
        val allFilterValues = mutableSetOf<QuadValue>()
        descriptor.subjects?.let { allFilterValues.addAll(it) }
        descriptor.predicates?.let { allFilterValues.addAll(it) }
        descriptor.objects?.let { allFilterValues.addAll(it) }
        descriptor.rangeFilters.forEach { allFilterValues.add(it.predicate) }
        descriptor.textFilters.forEach { allFilterValues.add(it.predicate) }
        descriptor.exclusionFilters.forEach {
            allFilterValues.add(it.predicate)
            allFilterValues.addAll(it.excludedValues)
        }

        // 2. Resolve all filter values to (typeId, longId) pairs in one batch
        val filterIds = getOrAddQuadValueIds(allFilterValues, false)

        // 3. Build single SQL statement
        val sql = buildMaterializedSql(descriptor, filterIds)

        // 4. Execute and convert to QuadSet using existing getIds pipeline
        val finalSeptuples = mutableSetOf<Pair<Long, Triple<QuadValueId, QuadValueId, QuadValueId>>>()

        transaction {
            exec(sql) { rs ->
                while (rs.next()) {
                    val id = rs.getLong("id")
                    val sType = rs.getInt("s_type")
                    val s = rs.getLong("s")
                    val pType = rs.getInt("p_type")
                    val p = rs.getLong("p")
                    val oType = rs.getInt("o_type")
                    val o = rs.getLong("o")

                    finalSeptuples.add(
                        id to Triple(
                            (sType to s),
                            (pType to p),
                            (oType to o)
                        )
                    )
                }
            }
        }

        val result = getIds(finalSeptuples)
        if (TIMING_ENABLED) logger.info("PostgresStore.materializeFilter: Returning ${result.size} quads in ${System.currentTimeMillis() - startTime}ms")
        return result
    }

    /**
     * Build SQL for a FilterDescriptor, choosing between simple and CTE-based approaches.
     * Simple descriptors use a straightforward query (fewer CTEs = faster planning).
     * Complex descriptors use CTEs for better composability and optimization.
     */
    private fun buildMaterializedSql(
        descriptor: FilterDescriptor,
        filterIds: Map<QuadValue, QuadValueId>
    ): String {
        return if (descriptor.isComposable) {
            buildSimpleMaterializedSql(descriptor, filterIds)
        } else {
            buildCteMaterializedSql(descriptor, filterIds)
        }
    }

    /**
     * Build a simple non-CTE SQL query for a FilterDescriptor.
     * This is the original approach — a single SELECT with JOINs and WHERE conditions.
     * Best for simple descriptors where CTE overhead is unnecessary.
     */
    private fun buildSimpleMaterializedSql(
        descriptor: FilterDescriptor,
        filterIds: Map<QuadValue, QuadValueId>
    ): String {
        val sb = StringBuilder()
        sb.append("SELECT q.id, q.s_type, q.s, q.p_type, q.p, q.o_type, q.o FROM quads q")

        // JOIN for range filters
        val needsRangeJoin = descriptor.rangeFilters.isNotEmpty()
        if (needsRangeJoin) {
            sb.append(" INNER JOIN literal_double d ON q.o = d.id AND q.o_type = $DOUBLE_LITERAL_TYPE")
        }

        // LEFT JOINs for ORDER BY on objects (only if no range JOIN already provides d)
        val needsObjectSort = descriptor.orderBy.any { it.component == QuadComponent.OBJECT }
        if (needsObjectSort && !needsRangeJoin) {
            sb.append(" LEFT JOIN literal_double ld ON q.o = ld.id AND q.o_type = $DOUBLE_LITERAL_TYPE")
            sb.append(" LEFT JOIN literal_string ls ON q.o = ls.id AND q.o_type = $STRING_LITERAL_TYPE")
        }

        val conditions = mutableListOf<String>()

        // Core set filters
        descriptor.subjects?.let { subjects ->
            val ids = subjects.mapNotNull { filterIds[it] }
            if (ids.isNotEmpty()) {
                conditions.add(buildOrChainForFilter("q.s_type", "q.s", ids))
            } else {
                conditions.add("FALSE")
            }
        }

        descriptor.predicates?.let { predicates ->
            val ids = predicates.mapNotNull { filterIds[it] }
            if (ids.isNotEmpty()) {
                conditions.add(buildOrChainForFilter("q.p_type", "q.p", ids))
            } else {
                conditions.add("FALSE")
            }
        }

        descriptor.objects?.let { objects ->
            val ids = objects.mapNotNull { filterIds[it] }
            if (ids.isNotEmpty()) {
                conditions.add(buildOrChainForFilter("q.o_type", "q.o", ids))
            } else {
                conditions.add("FALSE")
            }
        }

        // Range filters (predicate + value range)
        for (range in descriptor.rangeFilters) {
            val pId = filterIds[range.predicate] ?: continue
            conditions.add("(q.p_type = ${pId.first} AND q.p = ${pId.second})")
            conditions.add("q.o_type = $DOUBLE_LITERAL_TYPE")
            if (range.min != null) conditions.add("d.value >= ${range.min}")
            if (range.max != null) conditions.add("d.value <= ${range.max}")
        }

        // Text filters via dictionary text lookup
        for (tf in descriptor.textFilters) {
            val pId = filterIds[tf.predicate] ?: continue
            conditions.add("(q.p_type = ${pId.first} AND q.p = ${pId.second})")
            conditions.add("q.o_type = $STRING_LITERAL_TYPE")
            val textIds = dictionary.lookUpStringValueIdsByText(tf.searchText)
            if (textIds.isNotEmpty()) {
                conditions.add("q.o IN (${textIds.joinToString(",")})")
            } else {
                conditions.add("FALSE")
            }
        }

        // Exclusion filters — scoped to the predicate so only quads matching
        // both predicate AND excluded object values are removed.
        for (ef in descriptor.exclusionFilters) {
            val pId = filterIds[ef.predicate]
            val excludedIds = ef.excludedValues.mapNotNull { filterIds[it] }
            if (excludedIds.isNotEmpty() && pId != null) {
                if (excludedIds.size <= OR_CHAIN_THRESHOLD) {
                    val notConditions = excludedIds.joinToString(" OR ") {
                        "(q.p_type = ${pId.first} AND q.p = ${pId.second} AND q.o_type = ${it.first} AND q.o = ${it.second})"
                    }
                    conditions.add("NOT ($notConditions)")
                } else {
                    val valuesClause = excludedIds.joinToString(",") { "(${it.first},${it.second})" }
                    conditions.add("NOT EXISTS (SELECT 1 FROM (VALUES $valuesClause) AS ex(o_type, o_id) WHERE q.p_type = ${pId.first} AND q.p = ${pId.second} AND q.o_type = ex.o_type AND q.o = ex.o_id)")
                }
            }
        }

        if (conditions.isNotEmpty()) {
            sb.append(" WHERE ")
            sb.append(conditions.joinToString(" AND "))
        }

        // ORDER BY
        if (descriptor.orderBy.isNotEmpty()) {
            val orderClauses = descriptor.orderBy.map { spec ->
                val sortExpr = when (spec.component) {
                    QuadComponent.SUBJECT -> "(q.s_type, q.s)"
                    QuadComponent.PREDICATE -> "(q.p_type, q.p)"
                    QuadComponent.OBJECT -> {
                        if (needsRangeJoin) "d.value"
                        else "COALESCE(ld.value::TEXT, ls.value, q.o::TEXT)"
                    }
                }
                val direction = if (spec.ascending) "ASC" else "DESC"
                "$sortExpr $direction"
            }
            sb.append(" ORDER BY ${orderClauses.joinToString(", ")}")
        }

        if (descriptor.limit < Int.MAX_VALUE) {
            sb.append(" LIMIT ${descriptor.limit}")
        }
        if (descriptor.offset > 0) {
            sb.append(" OFFSET ${descriptor.offset}")
        }

        return sb.toString()
    }

    /**
     * Build a CTE-based SQL query for a FilterDescriptor.
     * CTEs (WITH clauses) enable better query planning for complex descriptors
     * by allowing PostgreSQL to materialize intermediate results.
     *
     * Currently delegates to buildSimpleMaterializedSql as a fallback.
     * The CTE structure is ready for future optimization stages.
     */
    private fun buildCteMaterializedSql(
        descriptor: FilterDescriptor,
        filterIds: Map<QuadValue, QuadValueId>
    ): String {
        // For now, delegate to the simple builder.
        // Future stages will emit CTE-based SQL here with:
        //   - Separate CTEs for each filter dimension (subjects, predicates, objects)
        //   - Materialized CTEs with explicit MATERIALIZED hints
        //   - Progressive refinement through CTE chains
        return buildSimpleMaterializedSql(descriptor, filterIds)
    }

    private fun buildOrChainForFilter(
        typeCol: String,
        idCol: String,
        ids: List<QuadValueId>
    ): String {
        if (ids.isEmpty()) return "FALSE"
        if (ids.size > OR_CHAIN_THRESHOLD) {
            val values = ids.joinToString(",") { "(${it.first},${it.second})" }
            return "(($typeCol, $idCol) IN (VALUES $values))"
        }
        val orChain = ids.joinToString(" OR ") { "($typeCol = ${it.first} AND $idCol = ${it.second})" }
        return "($orChain)"
    }

    override fun textFilter(predicate: QuadValue, objectFilterText: String): QuadSet {
        val predicatePair = getQuadValueId(predicate)

        if (predicatePair.first == null || predicatePair.second == null) { //unknown predicate, can't have matching quads
            return BasicQuadSet()
        }

       val textIds = dictionary.lookUpStringValueIdsByText(objectFilterText)

        val quadIds = mutableListOf<Long>()
        textIds.chunked(10000).forEach { chunk ->
            transaction {
                quadIds.addAll(
                    QuadsTable.select(QuadsTable.id).where { (QuadsTable.pType eq predicatePair.first!!) and (QuadsTable.p eq predicatePair.second!!) and (QuadsTable.oType eq STRING_LITERAL_TYPE) and (QuadsTable.o inList chunk) }
                        .map { it[QuadsTable.id] }
                )
            }
        }

        return getIds(quadIds)
    }

    /**
     * Optimized implementation that uses SQL DISTINCT to get unique object values.
     * This avoids fetching all rows and deduplicating in memory.
     */
    override fun distinctObjects(predicate: QuadValue): Set<QuadValue> {
        val startTime = if (TIMING_ENABLED) System.currentTimeMillis() else 0L

        val predicateId = getQuadValueId(predicate)
        if (predicateId.first == null || predicateId.second == null) {
            return emptySet()
        }

        // Query distinct (oType, o) pairs from the database
        val distinctObjectIds = transaction {
            QuadsTable
                .select(QuadsTable.oType, QuadsTable.o)
                .where { (QuadsTable.pType eq predicateId.first!!) and (QuadsTable.p eq predicateId.second!!) }
                .withDistinct()
                .map { it[QuadsTable.oType] to it[QuadsTable.o] }
                .toSet()
        }

        if (TIMING_ENABLED) {
            logger.info("distinctObjects: Found ${distinctObjectIds.size} distinct objects in ${System.currentTimeMillis() - startTime}ms")
        }

        // Convert IDs back to QuadValues
        val quadValues = getQuadValues(distinctObjectIds)

        if (TIMING_ENABLED) {
            logger.info("distinctObjects: Total time ${System.currentTimeMillis() - startTime}ms")
        }

        return quadValues.values.toSet()
    }

    /**
     * Optimized implementation that uses SQL DISTINCT to get unique subject values.
     * This avoids fetching all rows and deduplicating in memory.
     */
    override fun distinctSubjects(predicate: QuadValue): Set<QuadValue> {
        val startTime = if (TIMING_ENABLED) System.currentTimeMillis() else 0L

        val predicateId = getQuadValueId(predicate)
        if (predicateId.first == null || predicateId.second == null) {
            return emptySet()
        }

        // Query distinct (sType, s) pairs from the database
        val distinctSubjectIds = transaction {
            QuadsTable
                .select(QuadsTable.sType, QuadsTable.s)
                .where { (QuadsTable.pType eq predicateId.first!!) and (QuadsTable.p eq predicateId.second!!) }
                .withDistinct()
                .map { it[QuadsTable.sType] to it[QuadsTable.s] }
                .toSet()
        }

        if (TIMING_ENABLED) {
            logger.info("distinctSubjects: Found ${distinctSubjectIds.size} distinct subjects in ${System.currentTimeMillis() - startTime}ms")
        }

        // Convert IDs back to QuadValues
        val quadValues = getQuadValues(distinctSubjectIds)

        if (TIMING_ENABLED) {
            logger.info("distinctSubjects: Total time ${System.currentTimeMillis() - startTime}ms")
        }

        return quadValues.values.toSet()
    }

    override fun estimatePredicateCardinality(predicate: QuadValue): Long? {
        val pId = getQuadValueId(predicate)
        if (pId.first == null || pId.second == null) return null

        val key = pId.first!! to pId.second!!
        val cached = predicateCardinalityCache.getIfPresent(key)
        if (cached != null) return cached

        val count = transaction {
            exec("SELECT COUNT(*)::bigint FROM quads WHERE p_type = ${pId.first} AND p = ${pId.second}") { rs ->
                rs.next()
                rs.getLong(1)
            } ?: 0L
        }

        predicateCardinalityCache.put(key, count)
        return count
    }

    override val size: Int
        get() = transaction {
            exec("SELECT COUNT(*)::int FROM quads") { rs ->
                rs.next()
                rs.getInt(1)
            } ?: 0
        }

    override fun isEmpty(): Boolean = this.size == 0

    /**
     * Lazy cursor-based iterator that fetches quads from PostgreSQL in chunks
     * using keyset pagination (WHERE id > lastSeenId). Memory usage is bounded
     * to one chunk regardless of total table size, and the chunked resolution
     * naturally warms the idCache and value caches for subsequent queries.
     */
    override fun iterator(): MutableIterator<Quad> = PostgresCursorIterator()

    private inner class PostgresCursorIterator : MutableIterator<Quad> {


        private var lastSeenId = 0L
        private var currentChunk: List<Quad> = emptyList()
        private var chunkIndex = 0
        private var exhausted = false

        private fun fetchNextChunk() {
            if (exhausted) return

            val chunkIds = transaction {
                exec("SELECT id FROM quads WHERE id > $lastSeenId ORDER BY id LIMIT $CURSOR_CHUNK_SIZE") { rs ->
                    val ids = mutableListOf<Long>()
                    while (rs.next()) {
                        ids.add(rs.getLong(1))
                    }
                    ids
                } ?: emptyList()
            }

            if (chunkIds.isEmpty()) {
                exhausted = true
                return
            }

            lastSeenId = chunkIds.last()
            currentChunk = getIds(chunkIds).toList()
            chunkIndex = 0
        }

        override fun hasNext(): Boolean {
            if (chunkIndex < currentChunk.size) return true
            fetchNextChunk()
            return chunkIndex < currentChunk.size
        }

        override fun next(): Quad {
            if (!hasNext()) throw NoSuchElementException()
            return currentChunk[chunkIndex++]
        }

        override fun remove() {
            throw UnsupportedOperationException("Remove not supported on PostgresStore cursor iterator")
        }
    }

    override fun addAll(elements: Collection<Quad>): Boolean {
            if (elements.isEmpty()) {
                return true
            }

            val values = elements.flatMap {
                sequenceOf(
                    it.subject, it.predicate, it.`object`
                )
            }.toSet()

            val valueIdMap = getOrAddQuadValueIds(values)

            val quadIdMap = elements.mapNotNull {
                val s = valueIdMap[it.subject]
                val p = valueIdMap[it.predicate]
                val o = valueIdMap[it.`object`]

                if (s == null || p == null || o == null) {
                    System.err.println("${it.subject}: $s, ${it.predicate}: $p, ${it.`object`}: $o")
                    return@mapNotNull null
                }

                quadHash(s.first, s.second, p.first, p.second, o.first, o.second) to it
            }.toMap().toMutableMap()

            quadIdMap.keys.chunked(10000).forEach { chunk ->
                transaction {
                    QuadsTable.select(QuadsTable.hash).where { QuadsTable.hash inList chunk }.forEach {
                        quadIdMap.remove(it[QuadsTable.hash])
                    }
                }
            }

            if (quadIdMap.isEmpty()) {
                return false
            }

            transaction {
                QuadsTable.batchInsert(quadIdMap.values) {
                    val s = valueIdMap[it.subject]!!
                    val p = valueIdMap[it.predicate]!!
                    val o = valueIdMap[it.`object`]!!
                    this[QuadsTable.sType] = s.first
                    this[QuadsTable.s] = s.second
                    this[QuadsTable.pType] = p.first
                    this[QuadsTable.p] = p.second
                    this[QuadsTable.oType] = o.first
                    this[QuadsTable.o] = o.second
                    this[QuadsTable.hash] = quadHash(s.first, s.second, p.first, p.second, o.first, o.second)
                }
            }

            return true
        }

    override fun clear() {
        TODO("Not yet implemented")
    }

    override fun remove(element: Quad): Boolean {
        TODO("Not yet implemented")
    }

    override fun removeAll(elements: Collection<Quad>): Boolean {
        TODO("Not yet implemented")
    }

    override fun retainAll(elements: Collection<Quad>): Boolean {
        TODO("Not yet implemented")
    }

    override fun dump(writer: Writer, chunkSize: Int) {
        // Write header
        writer.write("subject\tpredicate\tobject\n")

        val totalQuads = size.toLong()
        var quadsProcessed = 0L

        if (totalQuads == 0L) {
            logger.info("No quads to dump.")
            return
        }

        // Use cursor-based pagination instead of loading all IDs into memory
        var lastSeenId = 0L
        while (true) {
            val chunkIds = transaction {
                exec("SELECT id FROM quads WHERE id > $lastSeenId ORDER BY id LIMIT $chunkSize") { rs ->
                    val ids = mutableListOf<Long>()
                    while (rs.next()) {
                        ids.add(rs.getLong(1))
                    }
                    ids
                } ?: emptyList()
            }

            if (chunkIds.isEmpty()) break

            lastSeenId = chunkIds.last()
            val quads = getIds(chunkIds).toList()
            for (quad in quads) {
                val formattedSubject = quad.subject.toString()
                // Skip localhost subjects if they are not LocalQuadValue
                if (formattedSubject.contains("localhost") && quad.subject !is LocalQuadValue) {
                    continue
                }
                val formattedPredicate = quad.predicate.toString()
                val formattedObject = formatQuadValueForTsv(quad.`object`)
                writer.write("$formattedSubject\t$formattedPredicate\t$formattedObject\n")
            }
            writer.flush()
            quadsProcessed += chunkIds.size
            val percentage = (quadsProcessed * 100) / totalQuads
            logger.info("Progress: $percentage% ($quadsProcessed / $totalQuads)")
        }
    }

    /**
     * Extracts the string representation of a QuadValue, including its type suffix,
     * and handles any necessary internal escaping for TSV (but no outer quoting if not needed).
     *
     * @param quadValue The QuadValue object.
     * @return The TSV-formatted string for the object column, including suffix and proper internal escaping.
     */
    fun formatQuadValueForTsv(quadValue: QuadValue): String {
        // Get the string representation of the QuadValue, including the desired suffix.
        val stringRepresentationIncludingSuffix = quadValue.toString()

        // Standard TSV rules still apply for delimiter (tab), newline, and quote character.
        val needsInternalEscapingAndPossiblyOuterQuoting =
            stringRepresentationIncludingSuffix.contains('\t') ||
                    stringRepresentationIncludingSuffix.contains('\n') ||
                    stringRepresentationIncludingSuffix.contains('"')

        if (needsInternalEscapingAndPossiblyOuterQuoting) {
            // Escape internal double quotes by doubling them
            val escapedValue = stringRepresentationIncludingSuffix.replace("\"", "\"\"")
            return "\"$escapedValue\""
        } else {
            // No special characters requiring quoting or escaping, return as is
            return stringRepresentationIncludingSuffix
        }
    }
}