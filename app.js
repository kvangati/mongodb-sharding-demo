/* ========================================================
   MongoDB Sharding Demo — app.js
   Simulates Range, Hashed, and Zone sharding strategies
   with configurable node sets and single/multi-region deployments
   ======================================================== */

// ===== STATE =====
const state = {
  strategy: 'range',
  shardKey: null,
  regionMode: 'single',
  shardCount: 2,
  nodesPerShard: 3,
  mongosCount: 1,
  documents: [],
  totalInserted: 0,
  animationSpeed: 3,       // 1=slow … 5=fast (insert + topology)
  routingSpeed: 3,         // 1=slow … 5=fast (query routing flow)
  isInserting: false,
  isBulkLoading: false,
};

// ===== CONFIGURATION DATA =====

const STRATEGIES = {
  range: {
    name: 'Range Sharding',
    description: 'Documents are divided into contiguous ranges based on the shard key value. Ideal for range-based queries but can create hotspots with monotonically increasing keys.',
    collection: 'orders',
    fields: ['order_id', 'customer_id', 'order_date', 'total', 'status', 'region'],
    shardKeys: [
      {
        key: 'customer_id',
        label: 'customer_id',
        optimal: true,
        reason: 'High cardinality, enables customer-range queries, distributes writes evenly across segments.',
        hotspot: false,
      },
      {
        key: 'order_date',
        label: 'order_date (ISO)',
        optimal: true,
        reason: 'Great for time-range queries. Use with a compound key to avoid write hotspots on the latest shard.',
        hotspot: false,
      },
      {
        key: 'order_id',
        label: 'order_id (sequential)',
        optimal: false,
        reason: 'Sequential integers always land on the max-value shard, causing a write hotspot and uneven distribution.',
        hotspot: true,
      },
    ],
  },
  hashed: {
    name: 'Hashed Sharding',
    description: 'A hash function is applied to the shard key before routing. Guarantees uniform write distribution but sacrifices range-query efficiency — every range query fans out to all shards.',
    collection: 'users',
    fields: ['user_id', 'email', 'username', 'country', 'created_at', 'plan'],
    shardKeys: [
      {
        key: '_id',
        label: '_id (hashed)',
        optimal: true,
        reason: "MongoDB's ObjectId already embeds a timestamp + random bytes — hashing it gives near-perfect uniform distribution.",
        hotspot: false,
      },
      {
        key: 'user_id',
        label: 'user_id (UUID v4)',
        optimal: true,
        reason: 'UUID v4 random bits produce excellent hash distribution across all shards with zero risk of hotspots.',
        hotspot: false,
      },
      {
        key: 'email',
        label: 'email (hashed)',
        optimal: true,
        reason: 'High cardinality + pseudo-random distribution after hashing. Queries on email must scatter to all shards.',
        hotspot: false,
      },
    ],
  },
  zone: {
    name: 'Zone Sharding',
    description: 'Zones (a.k.a. tag-aware sharding) pin shard key ranges to specific shards. Perfect for data-residency requirements (GDPR), latency optimization, and geographic partitioning.',
    collection: 'user_data',
    fields: ['user_id', 'region', 'country', 'created_at', 'payload', 'tier'],
    shardKeys: [
      {
        key: 'region',
        label: '{ region, _id }',
        optimal: true,
        reason: 'Compound prefix on region routes all writes for a geographic zone to the correct shard. The _id suffix prevents hotspots within a zone.',
        hotspot: false,
      },
      {
        key: 'country',
        label: '{ country, user_id }',
        optimal: true,
        reason: 'Country-level granularity for GDPR/data-residency compliance. Ensures EU user data never leaves Europe-zone shards.',
        hotspot: false,
      },
      {
        key: 'datacenter',
        label: '{ datacenter, timestamp }',
        optimal: false,
        reason: 'Good for DC-aware routing, but timestamp suffix can cause intra-zone hotspots. Prefer _id as the second component.',
        hotspot: true,
      },
    ],
  },
};

const ZONES = {
  single: [
    { id: 'us-east-1', label: 'us-east-1', region: null, color: '#3B82F6' },
    { id: 'us-west-2', label: 'us-west-2', region: null, color: '#A855F7' },
    { id: 'us-central', label: 'us-central-1', region: null, color: '#F59E0B' },
    { id: 'us-east-2', label: 'us-east-2', region: null, color: '#EF4444' },
  ],
  multi: [
    { id: 'americas', label: 'Americas', region: 'Americas', dc: 'us-east-1', color: '#3B82F6', zoneClass: 'zone-americas' },
    { id: 'europe', label: 'Europe', region: 'Europe', dc: 'eu-west-1', color: '#A855F7', zoneClass: 'zone-europe' },
    { id: 'asia', label: 'Asia-Pacific', region: 'Asia', dc: 'ap-southeast-1', color: '#F59E0B', zoneClass: 'zone-asia' },
    { id: 'americas2', label: 'Americas DR', region: 'Americas', dc: 'us-west-2', color: '#3B82F6', zoneClass: 'zone-americas' },
  ],
};

// ===== QUERY EXECUTOR STATE =====
let qeRunning = false;

// ===== QUERY DEFINITIONS =====
// Each entry: code(), explain(), type, getTargetShards(), filterDocs(shardDocs), explanation, isAggregate?
const QUERY_DEFS = {
  range: [
    {
      id: 'rng_point',
      name: 'Point Lookup — Shard Key',
      type: 'targeted',
      code: () => {
        const sk = state.shardKey;
        if (sk === 'customer_id') return `db.orders.find({ customer_id: "C025000" })`;
        if (sk === 'order_date') return `db.orders.find({ order_date: "2022-06-15" })`;
        return `db.orders.find({ order_id: 100050 })`;
      },
      explain: () => `IXSCAN → SINGLE_SHARD_ROUTE → RETURN`,
      getTargetShards: () => {
        const n = state.shardCount, sk = state.shardKey;
        if (sk === 'customer_id') { const seg = Math.ceil(99999 / n); return [Math.min(Math.floor(25000 / seg), n - 1)]; }
        if (sk === 'order_date') return [Math.max(0, Math.min(Math.floor(((2022 - 2020) / 5) * n), n - 1))];
        return [n - 1];
      },
      filterDocs: (shardDocs) => {
        const sk = state.shardKey;
        if (sk === 'customer_id') return shardDocs.filter(d => d.customer_id === 'C025000').slice(0, 8);
        if (sk === 'order_date') return shardDocs.filter(d => d.order_date?.startsWith('2022-06')).slice(0, 8);
        return shardDocs.filter(d => d.order_id === 100050).slice(0, 8);
      },
      explanation: 'Mongos resolves the exact chunk containing this key value and routes to one shard only — no scatter-gather overhead.',
    },
    {
      id: 'rng_in_shard_range',
      name: 'In-Shard Range Query',
      type: 'targeted',
      code: () => {
        const sk = state.shardKey;
        if (sk === 'customer_id') return `db.orders.find({\n  customer_id: { $gte: "C010000", $lte: "C020000" }\n}).sort({ customer_id: 1 }).limit(50)`;
        if (sk === 'order_date') return `db.orders.find({\n  order_date: { $gte: "2021-01-01", $lte: "2021-12-31" }\n}).sort({ order_date: 1 })`;
        return `db.orders.find({\n  order_id: { $gte: 100100, $lte: 100200 }\n}).limit(50)`;
      },
      explain: () => `IXSCAN [range] → SINGLE_SHARD_ROUTE → SORT`,
      getTargetShards: () => {
        const n = state.shardCount, sk = state.shardKey;
        if (sk === 'customer_id') { const seg = Math.ceil(99999 / n); return [Math.min(Math.floor(10000 / seg), n - 1)]; }
        if (sk === 'order_date') return [Math.max(0, Math.min(Math.floor(((2021 - 2020) / 5) * n), n - 1))];
        return [n - 1];
      },
      filterDocs: (shardDocs) => {
        const sk = state.shardKey;
        if (sk === 'customer_id') return shardDocs.filter(d => d.customer_id >= 'C010000' && d.customer_id <= 'C020000').slice(0, 8);
        if (sk === 'order_date') return shardDocs.filter(d => d.order_date >= '2021-01-01' && d.order_date <= '2021-12-31').slice(0, 8);
        return shardDocs.filter(d => d.order_id >= 100100 && d.order_id <= 100200).slice(0, 8);
      },
      explanation: 'Range falls entirely within one shard\'s chunk → mongos routes to that single shard. O(n) scan on one node only.',
    },
    {
      id: 'rng_cross_range',
      name: 'Cross-Shard Range Query',
      type: 'partial',
      code: () => {
        const sk = state.shardKey;
        if (sk === 'customer_id') return `db.orders.find({\n  customer_id: { $gte: "C000000", $lte: "C060000" }\n}).sort({ customer_id: 1 }).limit(100)`;
        if (sk === 'order_date') return `db.orders.find({\n  order_date: { $gte: "2020-01-01", $lte: "2023-12-31" }\n}).sort({ order_date: 1 }).limit(100)`;
        return `db.orders.find({\n  order_id: { $gte: 100000, $lte: 100500 }\n}).limit(100)`;
      },
      explain: () => `IXSCAN [range] → MULTI_SHARD_ROUTE → MERGE_SORT`,
      getTargetShards: () => {
        const n = state.shardCount;
        return Array.from({ length: Math.max(2, Math.ceil(n * 0.65)) }, (_, i) => i).slice(0, n);
      },
      filterDocs: (shardDocs) => {
        const sk = state.shardKey;
        if (sk === 'customer_id') return shardDocs.filter(d => d.customer_id >= 'C000000' && d.customer_id <= 'C060000').slice(0, 5);
        if (sk === 'order_date') return shardDocs.filter(d => d.order_date >= '2020-01-01' && d.order_date <= '2023-12-31').slice(0, 5);
        return shardDocs.slice(0, 5);
      },
      explanation: 'Range spans chunk boundaries → mongos routes to N shards in parallel, then merge-sorts results at the router before returning to the client.',
    },
    {
      id: 'rng_scatter',
      name: 'Non-Shard-Key Filter (Scatter)',
      type: 'scatter',
      code: () => `db.orders.find({\n  status: "shipped",\n  total: { $gt: 500 }\n}).sort({ total: -1 }).limit(50)`,
      explain: () => `COLLSCAN/IXSCAN → ALL_SHARDS → MERGE_SORT → LIMIT`,
      getTargetShards: () => Array.from({ length: state.shardCount }, (_, i) => i),
      filterDocs: (shardDocs) => shardDocs.filter(d => d.status === 'shipped' && d.total > 500).slice(0, 4),
      explanation: 'No shard key in predicate → mongos broadcasts to ALL shards. Each shard filters independently; results merge at mongos. Add a compound index {status, total} to speed up each shard\'s scan.',
    },
    {
      id: 'rng_aggregate',
      name: '$group Aggregation (Scatter + Merge)',
      type: 'scatter',
      isAggregate: true,
      code: () => `db.orders.aggregate([\n  { $match: { order_date: { $gte: "2023-01-01" } } },\n  { $group: {\n    _id: "$status",\n    count: { $sum: 1 },\n    revenue: { $sum: "$total" }\n  }},\n  { $sort: { revenue: -1 } }\n])`,
      explain: () => `ALL_SHARDS ($match+$group partial) → MONGOS_MERGE ($group final+$sort) → CLIENT`,
      getTargetShards: () => Array.from({ length: state.shardCount }, (_, i) => i),
      filterDocs: (shardDocs) => {
        const filtered = shardDocs.filter(d => d.order_date >= '2023-01-01');
        const grouped = {};
        filtered.forEach(d => {
          if (!grouped[d.status]) grouped[d.status] = { _id: d.status, count: 0, revenue: 0 };
          grouped[d.status].count++;
          grouped[d.status].revenue = +((grouped[d.status].revenue + (d.total || 0)).toFixed(2));
        });
        return Object.values(grouped);
      },
      explanation: 'Two-phase aggregation: each shard computes a partial $group; mongos merges the partial accumulators into the final result set. Only summary rows travel over the wire — not raw documents.',
    },
    {
      id: 'rng_lookup',
      name: '$lookup — Cross-Collection Join',
      type: 'scatter',
      isAggregate: true,
      code: () => `db.orders.aggregate([\n  { $match: { status: "delivered" } },\n  { $lookup: {\n    from: "customers",\n    localField: "customer_id",\n    foreignField: "_id",\n    as: "customer"\n  }},\n  { $project: {\n    customer_id: 1, total: 1,\n    name: { $first: "$customer.name" }\n  }}\n])`,
      explain: () => `ALL_SHARDS → $lookup(customers PRIMARY_SHARD) → MERGE → CLIENT`,
      getTargetShards: () => Array.from({ length: state.shardCount }, (_, i) => i),
      filterDocs: (shardDocs) => shardDocs.filter(d => d.status === 'delivered').slice(0, 4).map(d => ({
        _id: d._id, order_id: d.order_id, customer_id: d.customer_id,
        total: d.total, name: `Customer ${d.customer_id}`,
      })),
      explanation: '$lookup on a non-co-located collection forces scatter-gather + cross-shard document fetch from the primary shard. Consider materializing joins with Zone sharding for OLTP workloads.',
    },
  ],
  hashed: [
    {
      id: 'hash_point',
      name: 'Exact Match — Hashed Key',
      type: 'targeted',
      code: () => {
        const sk = state.shardKey;
        const v = sk === 'email'
          ? '"alice@example.com"'
          : sk === 'user_id'
            ? '"a1b2c3d4-e5f6-4abc-8def-123456789abc"'
            : 'ObjectId("6507abc123")';
        return `db.users.find({ ${sk}: ${v} })`;
      },
      explain: () => `HASH(key) → SINGLE_SHARD_ROUTE → IXSCAN → RETURN`,
      getTargetShards: () => {
        const v = state.shardKey === 'email' ? 'alice@example.com' : 'a1b2c3d4-test';
        return [fnv32(v) % state.shardCount];
      },
      filterDocs: (shardDocs) => shardDocs.slice(0, 3),
      explanation: 'Mongos applies the same hash function to the query value, computes the destination shard, and routes directly. One network hop — O(1) routing cost.',
    },
    {
      id: 'hash_in',
      name: '$in — Multi-Value Lookup',
      type: 'partial',
      code: () => {
        const sk = state.shardKey;
        if (sk === 'email') return `db.users.find({\n  email: { $in: [\n    "alice@example.com",\n    "bob@example.com",\n    "carol@example.com"\n  ]}\n})`;
        return `db.users.find({\n  ${sk}: { $in: [\n    "id_alpha_001",\n    "id_beta_002",\n    "id_gamma_003"\n  ]}\n})`;
      },
      explain: () => `HASH(each value) → DEDUP → PARALLEL_ROUTE(N shards) → MERGE`,
      getTargetShards: () => {
        const vals = ['alice@example.com', 'bob@example.com', 'carol@example.com'];
        return [...new Set(vals.map(v => fnv32(v) % state.shardCount))];
      },
      filterDocs: (shardDocs) => shardDocs.slice(0, 4),
      explanation: 'Mongos hashes each $in value to find its target shard. Duplicate shards are deduplicated — often hitting far fewer shards than scatter-gather.',
    },
    {
      id: 'hash_range',
      name: 'Range on Hashed Field (Scatter)',
      type: 'scatter',
      code: () => `// ⚠ Range queries on the hashed shard key scatter to ALL shards\ndb.users.find({\n  ${state.shardKey}: { $gt: "aaa_prefix", $lt: "zzz_prefix" }\n})`,
      explain: () => `HASH_RANGE → CANNOT_PRUNE → SCATTER_ALL → MERGE`,
      getTargetShards: () => Array.from({ length: state.shardCount }, (_, i) => i),
      filterDocs: (shardDocs) => shardDocs.slice(0, 4),
      explanation: 'Hashing destroys sort order. A range predicate on the shard key maps to arbitrary hash buckets across all shards — mongos cannot prune any shard and must broadcast.',
    },
    {
      id: 'hash_non_key',
      name: 'Non-Shard-Key Filter (Scatter)',
      type: 'scatter',
      code: () => `db.users.find({\n  plan: "enterprise",\n  country: "US"\n}).sort({ created_at: -1 }).limit(20)`,
      explain: () => `COLLSCAN → ALL_SHARDS → MERGE_SORT → LIMIT_20`,
      getTargetShards: () => Array.from({ length: state.shardCount }, (_, i) => i),
      filterDocs: (shardDocs) => shardDocs.filter(d => d.plan === 'enterprise' && d.country === 'US').slice(0, 5),
      explanation: 'Fields not in the shard key always trigger scatter-gather. Add a compound index {plan, country, created_at} on each shard to speed up per-shard filtering.',
    },
    {
      id: 'hash_aggregate',
      name: '$group Count Pipeline',
      type: 'scatter',
      isAggregate: true,
      code: () => `db.users.aggregate([\n  { $group: {\n    _id: "$plan",\n    total: { $sum: 1 },\n    countries: { $addToSet: "$country" }\n  }},\n  { $sort: { total: -1 } }\n])`,
      explain: () => `ALL_SHARDS ($group partial) → MONGOS_MERGE ($group final) → $sort → CLIENT`,
      getTargetShards: () => Array.from({ length: state.shardCount }, (_, i) => i),
      filterDocs: (shardDocs) => {
        const grouped = {};
        shardDocs.forEach(d => {
          if (!grouped[d.plan]) grouped[d.plan] = { _id: d.plan, total: 0, countries: new Set() };
          grouped[d.plan].total++;
          grouped[d.plan].countries.add(d.country);
        });
        return Object.values(grouped).map(g => ({ ...g, countries: [...g.countries] }));
      },
      explanation: 'Two-phase aggregation: each shard computes partial accumulators; mongos merges them into the final $group result. Only per-plan summaries travel over the wire.',
    },
    {
      id: 'hash_distinct',
      name: 'distinct() on Non-Key Field',
      type: 'scatter',
      code: () => `db.users.distinct("country")\n// Equivalent pipeline:\ndb.users.aggregate([\n  { $group: { _id: "$country" } },\n  { $sort: { _id: 1 } }\n])`,
      explain: () => `ALL_SHARDS (partial distinct) → MONGOS_MERGE (union + dedup) → CLIENT`,
      getTargetShards: () => Array.from({ length: state.shardCount }, (_, i) => i),
      filterDocs: (shardDocs) => {
        const countries = [...new Set(shardDocs.map(d => d.country))];
        return countries.map(c => ({ _id: c }));
      },
      isAggregate: true,
      explanation: 'distinct() fans out to all shards; each returns its local set of unique values. Mongos unions and de-duplicates across shard results before returning.',
    },
  ],
  zone: [
    {
      id: 'zone_targeted',
      name: 'Zone-Targeted Find',
      type: 'targeted',
      code: () => {
        const sk = state.shardKey;
        if (sk === 'country') return `db.user_data.find({\n  country: "DE",\n  tier: "pro"\n}).sort({ created_at: -1 }).limit(25)`;
        return `db.user_data.find({\n  region: "europe",\n  tier: "pro"\n}).sort({ created_at: -1 }).limit(25)`;
      },
      explain: () => `ZONE_KEY_MATCH → SINGLE_ZONE_SHARD → IXSCAN → RETURN`,
      getTargetShards: () => {
        if (state.regionMode === 'multi') return [Math.min(1, state.shardCount - 1)];
        const v = state.shardKey === 'country' ? 'DE' : 'europe';
        return [fnv32(v) % state.shardCount];
      },
      filterDocs: (shardDocs) => {
        const sk = state.shardKey;
        return shardDocs.filter(d => (sk === 'country' ? d.country === 'DE' : d.region === 'europe') && d.tier === 'pro').slice(0, 6);
      },
      explanation: 'Zone prefix in query → mongos resolves the zone tag and routes only to the Europe-zone shard(s). Zero data crosses region boundaries — optimal for GDPR compliance.',
    },
    {
      id: 'zone_scoped_agg',
      name: 'Zone-Scoped Aggregation',
      type: 'partial',
      isAggregate: true,
      code: () => {
        const matchKey = state.shardKey === 'country' ? 'country: { $in: ["DE","FR","GB","NL","ES"] }' : 'region: "europe"';
        return `db.user_data.aggregate([\n  { $match: { ${matchKey} } },\n  { $group: {\n    _id: "$country",\n    users: { $sum: 1 },\n    avg_tier: { $push: "$tier" }\n  }},\n  { $sort: { users: -1 } }\n])`;
      },
      explain: () => `ZONE_MATCH → EUROPE_SHARD(S) ONLY → $group partial → MONGOS_MERGE → CLIENT`,
      getTargetShards: () => {
        if (state.regionMode === 'multi') return [Math.min(1, state.shardCount - 1)];
        return [fnv32('europe') % state.shardCount];
      },
      filterDocs: (shardDocs) => {
        const eu = new Set(['DE', 'FR', 'GB', 'NL', 'ES', 'IT']);
        const filtered = shardDocs.filter(d => d.region === 'europe' || eu.has(d.country));
        const grouped = {};
        filtered.forEach(d => { grouped[d.country] = (grouped[d.country] || 0) + 1; });
        return Object.entries(grouped).map(([_id, users]) => ({ _id, users }));
      },
      explanation: 'Zone prefix in $match → mongos routes only to Europe-zone shards. Asia and Americas shards receive zero traffic — ideal for GDPR-scoped analytics.',
    },
    {
      id: 'zone_multi',
      name: 'Multi-Zone $in Query',
      type: 'partial',
      code: () => `db.user_data.find({\n  region: { $in: ["americas", "europe"] },\n  tier: { $in: ["pro", "enterprise"] }\n}).limit(100)`,
      explain: () => `ZONE_IN_MATCH → AMERICAS + EUROPE SHARDS → PARALLEL_ROUTE → MERGE`,
      getTargetShards: () => {
        if (state.regionMode === 'multi') return [0, Math.min(1, state.shardCount - 1)];
        return [...new Set(['americas', 'europe'].map(v => fnv32(v) % state.shardCount))];
      },
      filterDocs: (shardDocs) => shardDocs.filter(d => ['americas', 'europe'].includes(d.region) && ['pro', 'enterprise'].includes(d.tier)).slice(0, 5),
      explanation: '$in on zone prefix → mongos identifies 2 zone sets and routes only to those shards. Asia-Pacific shard(s) receive zero traffic for this query.',
    },
    {
      id: 'zone_global',
      name: 'Global Query (Scatter-Gather)',
      type: 'scatter',
      code: () => `db.user_data.find({\n  tier: "enterprise",\n  created_at: { $gte: "2024-01-01" }\n}).sort({ created_at: -1 }).limit(50)`,
      explain: () => `NO_ZONE_PREFIX → ALL_ZONES → SCATTER → MERGE_SORT → LIMIT`,
      getTargetShards: () => Array.from({ length: state.shardCount }, (_, i) => i),
      filterDocs: (shardDocs) => shardDocs.filter(d => d.tier === 'enterprise' && d.created_at >= '2024-01-01').slice(0, 4),
      explanation: 'No zone prefix → mongos cannot prune any zone. Query fans out globally. Use dedicated read replicas per zone and route analytics to regional read replicas to avoid cross-zone latency.',
    },
    {
      id: 'zone_lookup',
      name: '$lookup Across Zone Shards',
      type: 'scatter',
      isAggregate: true,
      code: () => `db.user_data.aggregate([\n  { $match: { tier: "enterprise" } },\n  { $lookup: {\n    from: "billing",\n    localField: "user_id",\n    foreignField: "user_id",\n    as: "billing"\n  }},\n  { $unwind: "$billing" },\n  { $project: {\n    user_id: 1, region: 1,\n    amount: "$billing.amount_due"\n  }}\n])`,
      explain: () => `ALL_ZONES (scatter) → $lookup PRIMARY_SHARD(billing) → MERGE → CLIENT`,
      getTargetShards: () => Array.from({ length: state.shardCount }, (_, i) => i),
      filterDocs: (shardDocs) => shardDocs.filter(d => d.tier === 'enterprise').slice(0, 4).map(d => ({
        _id: d._id, user_id: d.user_id, region: d.region,
        amount: +(Math.random() * 5000 + 500).toFixed(2),
      })),
      explanation: '$lookup on a non-zone-sharded collection fetches from the primary shard. Cross-zone $lookup incurs scatter latency + cross-region data movement. Co-locate billing with user_data using matching zone keys to avoid this.',
    },
  ],
};

// ===== FAKE DATA GENERATORS =====

const STATUSES = ['pending', 'processing', 'shipped', 'delivered', 'cancelled'];
const COUNTRIES = { americas: ['US', 'CA', 'BR', 'MX', 'AR'], europe: ['DE', 'FR', 'GB', 'NL', 'ES', 'IT'], asia: ['JP', 'SG', 'IN', 'AU', 'KR'] };
const PLANS = ['free', 'starter', 'pro', 'enterprise'];
const REGIONS = ['americas', 'europe', 'asia'];

let docSeq = 0;

function generateDocument() {
  docSeq++;
  const ts = Date.now();
  const base = { _id: `ObjectId("${randomHex(24)}")`, created_at: new Date(ts - Math.random() * 1e9).toISOString().split('T')[0] };

  if (state.strategy === 'range') {
    const region = REGIONS[Math.floor(Math.random() * REGIONS.length)];
    return {
      ...base,
      order_id: 100000 + docSeq,
      customer_id: `C${pad(Math.floor(Math.random() * 1000000), 6)}`,
      order_date: new Date(ts - Math.random() * 3e10).toISOString().split('T')[0],
      total: +(Math.random() * 2000 + 10).toFixed(2),
      status: STATUSES[Math.floor(Math.random() * STATUSES.length)],
      region,
    };
  }
  if (state.strategy === 'hashed') {
    return {
      ...base,
      user_id: 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
        const r = Math.random() * 16 | 0;
        return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
      }),
      email: `user${docSeq}@${['gmail', 'yahoo', 'outlook', 'company'][Math.floor(Math.random() * 4)]}.com`,
      username: `user_${randomHex(6)}`,
      country: ['US', 'DE', 'JP', 'BR', 'IN', 'GB'][Math.floor(Math.random() * 6)],
      plan: PLANS[Math.floor(Math.random() * PLANS.length)],
    };
  }
  // zone
  const region = REGIONS[Math.floor(Math.random() * REGIONS.length)];
  const countryPool = COUNTRIES[region] || COUNTRIES.americas;
  const country = countryPool[Math.floor(Math.random() * countryPool.length)];
  return {
    ...base,
    user_id: `U${pad(docSeq, 8)}`,
    region,
    country,
    datacenter: region === 'americas' ? 'us-east-1' : region === 'europe' ? 'eu-west-1' : 'ap-southeast-1',
    payload: `data_${randomHex(8)}`,
    tier: PLANS[Math.floor(Math.random() * PLANS.length)],
  };
}

function randomHex(len) {
  return Array.from({ length: len }, () => Math.floor(Math.random() * 16).toString(16)).join('');
}
function pad(n, w) { return String(n).padStart(w, '0'); }

// ===== SHARDING ALGORITHMS =====

function routeDocument(doc) {
  const key = state.shardKey;
  if (!key) return 0;

  if (state.strategy === 'hashed') {
    // MongoDB uses MD5 for hash sharding; we use FNV-1a + fmix32 (murmur3
    // finalisation) which has equivalent avalanche — ensures uniform % n.
    const val = doc[key === '_id' ? '_id' : key] || doc._id;
    return hashToShard(val, state.shardCount);
  }

  if (state.strategy === 'range') {
    const n = state.shardCount;
    if (key === 'customer_id') {
      const num = parseInt((doc.customer_id || '').replace('C', '')) || 0;
      const segSize = Math.ceil(999999 / n);
      return Math.min(Math.floor(num / segSize), n - 1);
    }
    if (key === 'order_date') {
      const d = new Date(doc.order_date || doc.created_at).getFullYear();
      // Spread years 2020-2025 across shards
      const span = 5;
      const seg = Math.floor(((d - 2020) / span) * n);
      return Math.max(0, Math.min(seg, n - 1));
    }
    if (key === 'order_id') {
      // Sequential → always goes to the last shard (hotspot demo)
      return n - 1;
    }
    return 0;
  }

  if (state.strategy === 'zone') {
    if (key === 'region') {
      const r = doc.region || 'americas';
      if (state.regionMode === 'multi') {
        if (r === 'europe') return Math.min(1, state.shardCount - 1);
        if (r === 'asia') return Math.min(2, state.shardCount - 1);
        // americas: split between shard 0 and shard 3 (DR) when 4 shards exist
        if (state.shardCount >= 4) return fmix32(fnv32(String(doc._id))) % 2 === 0 ? 0 : 3;
        return 0;
      }
      return fnv32(r) % state.shardCount;
    }
    if (key === 'country') {
      const c = doc.country || 'US';
      if (state.regionMode === 'multi') {
        const euCountries = new Set(['DE', 'FR', 'GB', 'NL', 'ES', 'IT']);
        const asiaCountries = new Set(['JP', 'SG', 'IN', 'AU', 'KR']);
        if (euCountries.has(c)) return Math.min(1, state.shardCount - 1);
        if (asiaCountries.has(c)) return Math.min(2, state.shardCount - 1);
        // Americas: split between shard 0 and shard 3 (DR) when 4 shards exist
        if (state.shardCount >= 4) return fmix32(fnv32(String(doc._id))) % 2 === 0 ? 0 : 3;
        return 0;
      }
      return fnv32(c) % state.shardCount;
    }
    // datacenter
    const dc = doc.datacenter || 'us-east-1';
    return fnv32(dc) % state.shardCount;
  }

  return 0;
}

// FNV-1a 32-bit hash (used for zone/misc routing)
function fnv32(str) {
  let hash = 2166136261;
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash = (hash * 16777619) >>> 0;
  }
  return hash;
}

// Murmur3 finalisation mix (fmix32) — dramatically improves bit avalanche.
// MongoDB uses MD5 for hash sharding; fmix32 is an equivalent quality finaliser.
// Raw FNV-1a has weak low-bit distribution for similar strings (user1@…, user2@…),
// which is why applying fmix32 after FNV is critical for even shard distribution.
function fmix32(h) {
  h = Math.imul(h ^ (h >>> 16), 0x85ebca6b) >>> 0;
  h = Math.imul(h ^ (h >>> 13), 0xc2b2ae35) >>> 0;
  return (h ^ (h >>> 16)) >>> 0;
}

// Hash a shard key value to a shard index.
// Uses FNV for initial hash then fmix32 for proper avalanche → uniform % n.
function hashToShard(val, n) {
  return fmix32(fnv32(String(val))) % n;
}

// ===== SHARD STATE =====

function getShards() {
  const count = state.shardCount;
  const zones = ZONES[state.regionMode];
  return Array.from({ length: count }, (_, i) => {
    const zone = zones[i % zones.length];
    const docs = state.documents.filter(d => d._shardIdx === i);
    return {
      index: i,
      name: `shard${i + 1}`,
      rsName: `rs${i + 1}`,
      zone,
      docs,
      docCount: docs.length,
    };
  });
}

function getRangeInfo(shardIdx) {
  const key = state.shardKey;
  const n = state.shardCount;
  if (state.strategy === 'range') {
    if (key === 'customer_id') {
      const seg = Math.ceil(999999 / n);
      const min = pad(shardIdx * seg, 6);
      const max = shardIdx === n - 1 ? '999999' : pad((shardIdx + 1) * seg - 1, 6);
      return `customer_id: C${min} → C${max}`;
    }
    if (key === 'order_date') {
      const span = 5;
      const yearStart = 2020 + Math.floor((shardIdx / n) * span);
      const yearEnd = 2020 + Math.floor(((shardIdx + 1) / n) * span);
      return `order_date: ${yearStart} → ${shardIdx === n - 1 ? 'MaxKey' : yearEnd}`;
    }
    if (key === 'order_id') {
      return shardIdx === n - 1 ? `order_id: ← HOTSPOT (all inserts here!)` : `order_id: no data (underutilized)`;
    }
  }
  if (state.strategy === 'hashed') {
    const chunkMin = shardIdx * Math.floor(0xFFFFFFFF / n);
    const chunkMax = shardIdx === n - 1 ? 'MaxKey' : (shardIdx + 1) * Math.floor(0xFFFFFFFF / n);
    return `hash(${key}): ${(chunkMin >>> 0).toString(16)} → ${typeof chunkMax === 'number' ? chunkMax.toString(16) : chunkMax}`;
  }
  if (state.strategy === 'zone') {
    const zone = ZONES[state.regionMode][shardIdx % ZONES[state.regionMode].length];
    if (state.regionMode === 'multi') {
      const zoneRanges = ['region: "americas"', 'region: "europe"', 'region: "asia"', 'region: "americas" (DR)'];
      return zoneRanges[shardIdx] || `zone: ${zone.label}`;
    }
    return `zone: ${zone.label}`;
  }
  return '';
}

// ===== UI RENDERERS =====

function renderStrategyInfo() {
  const s = STRATEGIES[state.strategy];
  document.getElementById('strategy-info').innerHTML = `<strong>${s.name}:</strong> ${s.description}`;
}

function renderShardKeys() {
  const keys = STRATEGIES[state.strategy].shardKeys;
  if (!state.shardKey || !keys.find(k => k.key === state.shardKey)) {
    state.shardKey = keys.find(k => k.optimal)?.key || keys[0].key;
  }
  const el = document.getElementById('shard-key-options');
  el.innerHTML = keys.map(k => `
    <div class="shard-key-option ${state.shardKey === k.key ? 'selected' : ''}" data-key="${k.key}">
      <div class="shard-key-radio"></div>
      <div class="shard-key-info">
        <div class="shard-key-name">
          ${k.label}
          ${k.optimal ? '<span class="optimal-badge">Optimal</span>' : ''}
          ${k.hotspot ? '<span class="hotspot-badge">Hotspot Risk</span>' : ''}
        </div>
        <div class="shard-key-reason">${k.reason}</div>
      </div>
    </div>
  `).join('');
  el.querySelectorAll('.shard-key-option').forEach(el => {
    el.addEventListener('click', () => {
      state.shardKey = el.dataset.key;
      state.documents = [];
      renderAll();
      addLog('info', `Shard key changed to <strong>${state.shardKey}</strong>. Data cleared.`);
    });
  });
}

function renderRegionDetail() {
  const el = document.getElementById('region-detail');
  if (state.regionMode === 'single') {
    el.textContent = 'All shards reside within a single region. Lower latency between nodes; no cross-region replication costs.';
  } else {
    el.innerHTML = `
      <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:6px;">
        <span class="zone-tag zone-americas">Americas (us-east-1 / us-west-2)</span>
        <span class="zone-tag zone-europe">Europe (eu-west-1)</span>
        <span class="zone-tag zone-asia">Asia-Pacific (ap-southeast-1)</span>
      </div>
      <div style="margin-top:8px;color:var(--text-muted);font-size:11px;">Data is pinned to regional zones. Ideal for GDPR compliance and latency-sensitive workloads.</div>
    `;
  }
}

function renderTopology() {
  const shards = getShards();
  const container = document.getElementById('topology-container');

  const mongosCards = Array.from({ length: state.mongosCount }, (_, i) => `
    <div class="node-card mongos" id="mongos-${i}">
      <span class="node-type-badge badge-mongos">mongos</span>
      <div class="node-name">mongos${i + 1}</div>
      <div class="node-detail">Query Router</div>
      <div class="node-status"><span class="status-dot"></span> Active</div>
    </div>
  `).join('');

  const configNodes = state.nodesPerShard === 1 ? 1 : 3;
  const configCards = Array.from({ length: configNodes }, (_, i) => `
    <div class="node-card configsvr">
      <span class="node-type-badge badge-config">configsvr</span>
      <div class="node-name">cfg${i + 1}</div>
      <div class="node-detail">Config RS ${i === 0 ? '(Primary)' : '(Secondary)'}</div>
      <div class="node-status"><span class="status-dot secondary"></span> Online</div>
    </div>
  `).join('');

  // Build shard cards
  const shardHTML = buildShardHTML(shards);

  // Connector SVG
  const connSVG = `<svg width="100%" height="24" xmlns="http://www.w3.org/2000/svg">
    <line x1="50%" y1="0" x2="50%" y2="24" stroke="#30363D" stroke-width="1.5" stroke-dasharray="4,3"/>
  </svg>`;

  container.innerHTML = `
    <div class="topo-section">
      <div class="topo-section-label">Application Layer</div>
      <div class="topo-row">
        <div class="node-card app-node">
          <span class="node-type-badge badge-app" style="margin:0 auto 4px;">Application</span>
          <div class="node-name" style="text-align:center">${STRATEGIES[state.strategy].collection}</div>
          <div class="node-detail" style="text-align:center">MongoDB Driver</div>
        </div>
      </div>
    </div>

    <div class="connector-multi">${connSVG}</div>

    <div class="topo-section">
      <div class="topo-section-label">Query Routing Layer (mongos)</div>
      <div class="topo-row">${mongosCards}</div>
    </div>

    <div class="connector-multi">${connSVG}</div>

    <div class="topo-section">
      <div class="topo-section-label">Config Server Replica Set (CSRS)</div>
      <div class="topo-row">${configCards}</div>
    </div>

    <div class="connector-multi">${connSVG}</div>

    <div class="topo-section">
      <div class="topo-section-label">
        Shard Replica Sets (${shards.length} shards × ${state.nodesPerShard} nodes)
      </div>
      ${shardHTML}
    </div>
  `;
}

function buildShardHTML(shards) {
  const shardCards = shards.map(s => buildShardCard(s)).join('');

  if (state.strategy === 'zone' && state.regionMode === 'multi') {
    const zoneMap = {};
    shards.forEach(s => {
      const zone = s.zone.region || s.zone.label;
      if (!zoneMap[zone]) zoneMap[zone] = [];
      zoneMap[zone].push(s);
    });
    return Object.entries(zoneMap).map(([zone, zoneShards]) => {
      const borderColor = zone.toLowerCase().includes('amer') ? 'rgba(59,130,246,0.4)'
        : zone.toLowerCase().includes('eur') ? 'rgba(168,85,247,0.4)' : 'rgba(245,158,11,0.4)';
      const textColor = zone.toLowerCase().includes('amer') ? '#3B82F6'
        : zone.toLowerCase().includes('eur') ? '#A855F7' : '#F59E0B';
      return `
        <div class="zone-region-wrapper" style="--zone-border:${borderColor}; margin-bottom:12px;">
          <div class="zone-region-label" style="color:${textColor};">${zone}</div>
          <div class="shards-container" style="margin-top:8px;">
            ${zoneShards.map(s => buildShardCard(s)).join('')}
          </div>
        </div>
      `;
    }).join('');
  }

  return `<div class="shards-container">${shardCards}</div>`;
}

function buildShardCard(s) {
  const nodes = buildNodeList(s);
  const rangeInfo = getRangeInfo(s.index);
  const isHotspot = state.strategy === 'range' && state.shardKey === 'order_id' && s.index === state.shardCount - 1;

  const zoneTag = state.strategy === 'zone' && state.regionMode === 'multi'
    ? `<div class="zone-tag ${s.zone.zoneClass || ''}" style="margin-top:6px;">
        <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
          <circle cx="12" cy="12" r="10"/></svg>
        ${s.zone.dc || s.zone.label}
      </div>` : '';

  const totalDocs = state.documents.length;
  const fillPct = totalDocs > 0 ? Math.round((s.docCount / totalDocs) * 100) : 0;
  const barColor = `var(--shard-${s.index % 4})`;

  return `
    <div class="shard-card" id="shard-card-${s.index}" data-shard="${s.index % 4}">
      <div class="shard-header">
        <div class="shard-name">${s.rsName} / ${s.name}</div>
        <div class="shard-doc-count" id="doc-count-${s.index}">${s.docCount} docs</div>
      </div>
      <div class="shard-nodes">${nodes}</div>
      <div class="shard-range-info">
        <span class="range-label">${rangeInfo}</span>
      </div>
      ${zoneTag}
      ${isHotspot ? '<div style="font-size:10px;color:#EF4444;margin-top:6px;font-weight:600;">⚠ WRITE HOTSPOT</div>' : ''}
      <div class="shard-fill-bar">
        <div class="shard-fill-inner" id="fill-${s.index}" style="width:${fillPct}%;background:${barColor};"></div>
      </div>
    </div>
  `;
}

function buildNodeList(s) {
  const n = state.nodesPerShard;
  const nodes = [];
  nodes.push({ role: 'PRIMARY', name: `${s.name}-p1`, dot: 'dot-primary', badge: 'badge-primary' });
  const secCount = n <= 1 ? 0 : n - 1;
  for (let i = 0; i < secCount; i++) {
    nodes.push({ role: 'SECONDARY', name: `${s.name}-s${i + 1}`, dot: 'dot-secondary', badge: 'badge-secondary' });
  }
  return nodes.map(node => `
    <div class="shard-node">
      <div class="shard-node-dot ${node.dot}"></div>
      <span class="shard-node-name">${node.name}</span>
      <span class="node-type-badge ${node.badge}">${node.role}</span>
    </div>
  `).join('');
}

function renderDistribution() {
  const shards = getShards();
  const totalDocs = state.documents.length;
  const maxDocs = Math.max(...shards.map(s => s.docCount), 1);

  const COLORS = ['#3B82F6', '#A855F7', '#F59E0B', '#EF4444'];

  // Balance score: 100% = perfect, 0% = all on one shard
  const perfect = totalDocs / Math.max(shards.length, 1);
  const deviations = shards.map(s => Math.abs(s.docCount - perfect));
  const maxDeviation = perfect * (shards.length - 1);
  const balanceScore = maxDeviation > 0 ? Math.max(0, 100 - (deviations.reduce((a, b) => a + b, 0) / (2 * Math.max(maxDeviation, 1))) * 100) : 100;
  const balanceColor = balanceScore >= 80 ? '#00ED64' : balanceScore >= 50 ? '#F59E0B' : '#EF4444';

  const barsHTML = shards.map((s, i) => {
    const pct = maxDocs > 0 ? (s.docCount / maxDocs) * 100 : 0;
    const color = COLORS[i % COLORS.length];
    return `
      <div class="dist-bar-wrap">
        <div class="dist-bar-outer">
          <div class="dist-bar-inner" style="height:${pct}%;background:${color}">
            ${s.docCount > 0 ? `<span class="dist-bar-count">${s.docCount}</span>` : ''}
          </div>
        </div>
        <div class="dist-bar-label">${s.rsName}</div>
        <div class="dist-bar-sub">${s.zone.label}</div>
      </div>
    `;
  }).join('');

  // Chunk map only for range sharding
  const chunkMapHTML = state.strategy === 'range' ? buildChunkMap(shards) : '';

  document.getElementById('distribution-panel').innerHTML = `
    <div class="dist-header">
      <div>
        <div class="dist-title">Data Distribution</div>
        <div class="dist-subtitle">${STRATEGIES[state.strategy].name} — Key: <code style="font-family:monospace;color:var(--mongo-green);">${state.shardKey || 'none'}</code></div>
      </div>
      <div class="dist-stats">
        <div class="dist-stat">
          <div class="dist-stat-value">${totalDocs}</div>
          <div class="dist-stat-label">Total Docs</div>
        </div>
        <div class="dist-stat">
          <div class="dist-stat-value">${shards.length}</div>
          <div class="dist-stat-label">Shards</div>
        </div>
        <div class="dist-stat">
          <div class="dist-stat-value" style="color:${balanceColor}">${totalDocs > 0 ? balanceScore.toFixed(0) + '%' : '—'}</div>
          <div class="dist-stat-label">Balance</div>
        </div>
      </div>
    </div>

    <div class="dist-chart-container">
      <div class="dist-chart-title">Documents per Shard</div>
      <div class="dist-bars">${barsHTML}</div>
    </div>

    ${chunkMapHTML}

    <div class="balance-indicator">
      <div class="balance-label">Distribution Balance</div>
      <div class="balance-bar">
        <div class="balance-fill" style="width:${totalDocs > 0 ? balanceScore : 0}%;background:${balanceColor};"></div>
      </div>
      <div class="balance-value" style="color:${balanceColor}">${totalDocs > 0 ? balanceScore.toFixed(1) + '%' : 'N/A'}</div>
    </div>
    ${state.strategy === 'range' && state.shardKey === 'order_id' ? `
      <div style="background:rgba(239,68,68,0.08);border:1px solid rgba(239,68,68,0.3);border-radius:8px;padding:12px 16px;margin-top:12px;font-size:12px;color:#EF4444;">
        <strong>⚠ Hotspot Detected:</strong> Sequential order_id values always route to the last shard.
        Switch to <code style="font-family:monospace;">customer_id</code> or <code style="font-family:monospace;">order_date</code> for balanced distribution.
      </div>
    ` : ''}
  `;
}

function buildChunkMap(shards) {
  const COLORS = ['#3B82F6', '#A855F7', '#F59E0B', '#EF4444'];
  // For hotspot key, make the visual reflect the skew
  let widths;
  if (state.shardKey === 'order_id' && state.strategy === 'range') {
    widths = shards.map((_, i) => i === shards.length - 1 ? 70 : (30 / (shards.length - 1)));
  } else {
    widths = shards.map(() => 100 / shards.length);
  }

  const segments = shards.map((s, i) => `
    <div class="chunk-segment" style="width:${widths[i]}%;background:${COLORS[i % COLORS.length]}">
      ${widths[i] > 8 ? `${s.name}` : ''}
    </div>
  `).join('');

  const legend = shards.map((s, i) => `
    <div class="chunk-legend-item">
      <div class="chunk-legend-dot" style="background:${COLORS[i % COLORS.length]}"></div>
      <span>${s.name}: ${getRangeInfo(i).replace(/^[^:]+:\s*/, '')}</span>
    </div>
  `).join('');

  return `
    <div class="chunk-map">
      <div class="chunk-map-title">Chunk Range Map (${state.shardKey})</div>
      <div class="chunk-track">${segments}</div>
      <div class="chunk-legend">${legend}</div>
    </div>
  `;
}

// ===== QUERY EXECUTOR HELPERS =====

function syntaxHighlight(code) {
  // Escape HTML first
  const escaped = code.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  // Single-pass tokenizer — each alternative is tried in order at every position.
  // This prevents later passes from matching inside HTML we just inserted.
  const TOKEN_RE = /(\/\/[^\n]*)|("(?:[^"\\]|\\.)*")|\b(db|find|aggregate|insertOne|updateOne|deleteOne|sort|limit|skip|distinct|count)\b|(\$[a-zA-Z_][a-zA-Z0-9_]*)|\b(\d+(?:\.\d+)?)\b/g;
  return escaped.replace(TOKEN_RE, (m, comment, str, fn, op, num) => {
    if (comment) return `<span class="qe-comment">${comment}</span>`;
    if (str)     return `<span class="qe-str">${str}</span>`;
    if (fn)      return `<span class="qe-fn">${fn}</span>`;
    if (op)      return `<span class="qe-op">${op}</span>`;
    if (num)     return `<span class="qe-num">${num}</span>`;
    return m;
  });
}

function setExecStatus(msg, cls) {
  const el = document.getElementById('qe-exec-status');
  if (!el) return;
  el.textContent = msg;
  el.className = `qe-exec-status ${cls || ''}`;
}

function setExecRowState(shardIdx, stateClass, label) {
  const el = document.getElementById(`qe-exec-state-${shardIdx}`);
  if (el) { el.className = `qe-exec-state ${stateClass}`; el.textContent = label; }
}

function setExecRowDone(shardIdx, matched, examined, latencyMs, color) {
  setExecRowState(shardIdx, 'state-done', 'done');
  const docsEl = document.getElementById(`qe-exec-docs-${shardIdx}`);
  const latEl = document.getElementById(`qe-exec-lat-${shardIdx}`);
  const exEl = document.getElementById(`qe-exec-ex-${shardIdx}`);
  const bar = document.getElementById(`qe-exec-bar-${shardIdx}`);
  if (docsEl) docsEl.textContent = `${matched} doc${matched !== 1 ? 's' : ''}`;
  if (latEl) latEl.textContent = `${latencyMs}ms`;
  if (exEl) exEl.textContent = `${examined} examined`;
  if (bar) { bar.style.width = '100%'; bar.style.background = color; }
}

function renderExecRows(shards) {
  const COLORS = ['#3B82F6', '#A855F7', '#F59E0B', '#EF4444'];
  return shards.map((s, i) => `
    <div class="qe-exec-row" id="qe-exec-row-${i}">
      <div class="qe-exec-shard" style="color:${COLORS[i % 4]}">${s.rsName}</div>
      <div class="qe-exec-state state-idle" id="qe-exec-state-${i}">idle</div>
      <div class="qe-exec-bar-wrap">
        <div class="qe-exec-bar" id="qe-exec-bar-${i}" style="background:${COLORS[i % 4]}"></div>
      </div>
      <div class="qe-exec-num" id="qe-exec-docs-${i}">—</div>
      <div class="qe-exec-num qe-exec-ex" id="qe-exec-ex-${i}">—</div>
      <div class="qe-exec-num" id="qe-exec-lat-${i}">—</div>
    </div>
  `).join('');
}

function formatDocValue(v) {
  if (v === null || v === undefined) return '<span class="doc-bool">null</span>';
  if (typeof v === 'boolean') return `<span class="doc-bool">${v}</span>`;
  if (typeof v === 'number') return `<span class="doc-num">${v}</span>`;
  if (Array.isArray(v)) return `[${v.slice(0, 3).map(formatDocValue).join(', ')}${v.length > 3 ? ', …' : ''}]`;
  if (typeof v === 'object') return `{…}`;
  const s = String(v);
  return `"<span class="doc-str">${s.length > 40 ? s.slice(0, 40) + '…' : s}</span>"`;
}

function formatDoc(doc, shardIdx) {
  const COLORS = ['#3B82F6', '#A855F7', '#F59E0B', '#EF4444'];
  const shards = getShards();
  const tag = shardIdx !== undefined
    ? `<span class="doc-shard-tag" style="background:${COLORS[shardIdx % 4]}22;color:${COLORS[shardIdx % 4]}">${shards[shardIdx]?.rsName || ''}</span>`
    : '';
  const fields = Object.entries(doc).slice(0, 7)
    .map(([k, v]) => `  <span class="doc-key">"${k}"</span>: ${formatDocValue(v)}`)
    .join(',\n');
  return `<div class="qe-result-doc"><span class="doc-brace">{</span>${tag}\n${fields}\n<span class="doc-brace">}</span></div>`;
}

async function executeQueryAnimation() {
  if (qeRunning) return;
  qeRunning = true;

  const queries = QUERY_DEFS[state.strategy];
  const selectedId = document.getElementById('qe-select')?.value;
  const queryDef = queries.find(q => q.id === selectedId) || queries[0];
  const shards = getShards();
  const COLORS = ['#3B82F6', '#A855F7', '#F59E0B', '#EF4444'];
  const speed = state.routingSpeed ?? state.animationSpeed;
  const delay = Math.max(80, 500 - speed * 80);

  const btn = document.getElementById('qe-run-btn');
  if (btn) { btn.disabled = true; btn.innerHTML = '<span style="opacity:0.7">⏳</span> Running…'; }

  // Reset all rows to idle
  shards.forEach((_, i) => setExecRowState(i, 'state-idle', 'idle'));
  document.getElementById('qe-exec-summary').innerHTML = '';
  document.getElementById('qe-results').innerHTML = '<div class="qe-results-empty">Executing…</div>';
  qfResetAll();
  qfStepsReset();

  const typeLabel = { targeted: 'Targeted', scatter: 'Scatter-Gather', partial: 'Partial' }[queryDef.type] || queryDef.type;
  const shardNames = i => shards[i]?.rsName || `shard${i + 1}`;

  const targetShards = queryDef.getTargetShards();
  const skippedShards = shards.map((_, i) => i).filter(i => !targetShards.includes(i));

  // Dim non-target shards in the flow diagram up front
  skippedShards.forEach(i => {
    const n = document.getElementById(`qf-shard-${i}`);
    if (n) n.classList.add('qf-dim');
    const l = document.getElementById(`qf-line-shard-${i}`);
    if (l) l.classList.add('qf-dim');
  });

  // ----- PHASE 1: Client → Mongos -----
  setExecStatus('Planning query…', 'planning');
  qfSetPhase('Client submits query → mongos', 'phase-plan');
  qfStep('sends query over the wire to a <code>mongos</code> router', {
    actor: 'client', actorColor: '#00ED64', variant: 'client',
    detail: `query type: <strong>${typeLabel}</strong> · collection: <code>${STRATEGIES[state.strategy].collection}</code>`,
  });
  qfActivate('qf-client', delay * 0.6);
  qfHighlightLine('qf-line-cm', '#00ED64', delay * 0.9);
  await qfPacket('qf-client', 'qf-mongos', '#00ED64', delay * 0.85);
  qfActivate('qf-mongos', delay * 0.6);
  qfStep('receives connection, parses BSON, extracts shard-key predicate', {
    actor: 'mongos', variant: 'mongos',
    detail: `shard key: <code>${state.shardKey || '—'}</code>`,
  });

  // ----- PHASE 2: Mongos ↔ Config Server (consult chunk map) -----
  qfSetPhase('mongos consults config server for chunk metadata', 'phase-meta');
  qfStep('requests chunk map from the <strong>Config Server</strong>', {
    actor: 'mongos', variant: 'meta',
    detail: `reads <code>config.chunks</code> / <code>config.shards</code> (cached locally ~30s)`,
  });
  qfHighlightLine('qf-line-mc', '#F59E0B', delay * 1.4);
  await qfPacket('qf-mongos', 'qf-config', '#F59E0B', delay * 0.65);
  qfActivate('qf-config', delay * 0.8);
  await sleep(delay * 0.2);
  qfStep('returns chunk ranges and owning shards', {
    actor: 'config server', variant: 'meta',
    detail: `${shards.length} shards · ${shards.length * 2}+ chunks`,
  });
  await qfPacket('qf-config', 'qf-mongos', '#F59E0B', delay * 0.65);
  qfActivate('qf-mongos', delay * 0.5);

  document.querySelectorAll('.node-card.mongos').forEach(n => n.classList.add('active-routing'));
  await sleep(delay * 0.3);
  document.querySelectorAll('.node-card.mongos').forEach(n => n.classList.remove('active-routing'));

  // Plan step: decide targeting
  const planMsg = targetShards.length === 1
    ? `resolves predicate to a single chunk → routes to <strong>${shardNames(targetShards[0])}</strong>`
    : targetShards.length === shards.length
      ? `predicate lacks shard-key prefix → <strong>scatter-gather</strong> to all ${shards.length} shards`
      : `predicate spans ${targetShards.length} chunk ranges → <strong>partial broadcast</strong> to ${targetShards.length} of ${shards.length} shards`;
  qfStep(planMsg, {
    actor: 'mongos', variant: 'plan',
    detail: `targets: ${targetShards.map(i => `<code style="color:${COLORS[i % 4]}">${shardNames(i)}</code>`).join(', ')}${skippedShards.length ? ` · skips: ${skippedShards.map(shardNames).join(', ')}` : ''}`,
  });

  // Mark non-targeted shards as skipped on the exec table
  skippedShards.forEach(i => setExecRowState(i, 'state-skipped', 'skipped'));

  // ----- PHASE 3: Mongos → target shards (dispatch) -----
  const phaseLabel = targetShards.length === 1
    ? `Targeted routing → 1 shard`
    : `Scatter to ${targetShards.length} shards`;
  setExecStatus(`Routing to ${targetShards.length} shard(s)…`, 'routing');
  qfSetPhase(phaseLabel, 'phase-dispatch');
  qfStep(`dispatches query to ${targetShards.length} shard${targetShards.length !== 1 ? 's' : ''} in parallel`, {
    actor: 'mongos', variant: 'dispatch',
    detail: `fan-out over ${targetShards.length} persistent connection${targetShards.length !== 1 ? 's' : ''}`,
  });

  await Promise.all(targetShards.map(async (shardIdx, i) => {
    await sleep(i * delay * 0.15);
    const color = COLORS[shardIdx % 4];
    qfHighlightLine(`qf-line-shard-${shardIdx}`, color, delay * 1.1);
    qfPacket('qf-mongos', `qf-shard-${shardIdx}`, color, delay * 0.8);
  }));
  await sleep(delay * 0.85);

  // ----- PHASE 4: Shards execute in parallel -----
  qfSetPhase(
    targetShards.length === 1
      ? 'Shard executes query against its local data'
      : 'Each target shard executes the query in parallel',
    'phase-exec'
  );
  const results = {};
  await Promise.all(targetShards.map(async (shardIdx, i) => {
    await sleep(i * delay * 0.2);
    setExecRowState(shardIdx, 'state-executing', 'executing');
    qfActivate(`qf-shard-${shardIdx}`, delay * 1.1, COLORS[shardIdx % 4]);
    const card = document.getElementById(`shard-card-${shardIdx}`);
    if (card) card.classList.add('active-routing');

    await sleep(delay * 0.9 + Math.random() * delay * 0.4);

    const shardDocs = shards[shardIdx]?.docs || [];
    const matched = queryDef.filterDocs(shardDocs);
    const latencyMs = 2 + Math.floor(shardDocs.length * 0.05 + Math.random() * 14);

    if (card) card.classList.remove('active-routing');
    results[shardIdx] = { matched, examined: shardDocs.length, latencyMs };
    setExecRowDone(shardIdx, matched.length, shardDocs.length, latencyMs, COLORS[shardIdx % 4]);

    qfStep(`executes on primary, scans ${shardDocs.length} docs, matches <strong>${matched.length}</strong>`, {
      actor: shardNames(shardIdx), actorColor: COLORS[shardIdx % 4], variant: 'exec',
      detail: `${queryDef.isAggregate ? 'partial $group/$match' : 'IXSCAN/COLLSCAN'} · ${latencyMs} ms on-shard`,
    });
  }));

  // ----- PHASE 5: Shards → Mongos (return partial results) -----
  qfSetPhase(
    targetShards.length === 1
      ? 'Shard returns results to mongos'
      : 'Shards return partial results to mongos',
    'phase-return'
  );
  await Promise.all(targetShards.map(async (shardIdx, i) => {
    await sleep(i * delay * 0.1);
    const color = COLORS[shardIdx % 4];
    qfHighlightLine(`qf-line-shard-${shardIdx}`, color, delay * 0.9);
    const matchedCount = results[shardIdx]?.matched?.length || 0;
    qfPacket(`qf-shard-${shardIdx}`, 'qf-mongos', color, delay * 0.8, {
      label: matchedCount > 0 ? `${matchedCount}` : '',
    });
  }));
  await sleep(delay * 0.85);
  qfActivate('qf-mongos', delay * 0.8);
  qfStep(`receives partial results from ${targetShards.length} shard${targetShards.length !== 1 ? 's' : ''}`, {
    actor: 'mongos', variant: 'return',
    detail: targetShards.map(i => `<code style="color:${COLORS[i % 4]}">${shardNames(i)}: ${results[i]?.matched?.length || 0}</code>`).join(' · '),
  });

  // ----- PHASE 6: Merge at mongos (multi-shard only) -----
  if (targetShards.length > 1) {
    setExecStatus('Merging results at mongos…', 'merging');
    qfSetPhase('mongos merges partial results (sort / limit / dedupe)', 'phase-merge');
    qfStep(`merges partial results${queryDef.isAggregate ? ' and runs final $group/$sort stage' : ' (merge-sort, apply limit/skip)'}`, {
      actor: 'mongos', variant: 'merge',
      detail: `combined ${targetShards.reduce((s, i) => s + (results[i]?.matched?.length || 0), 0)} docs from ${targetShards.length} shards`,
    });
    document.querySelectorAll('.node-card.mongos').forEach(n => n.classList.add('active-routing'));
    await sleep(delay * 0.7);
    document.querySelectorAll('.node-card.mongos').forEach(n => n.classList.remove('active-routing'));
  }

  // Compute totals
  const allDocs = targetShards.flatMap(i => (results[i]?.matched || []).map(d => ({ ...d, _shardIdx: i })));
  const totalExamined = targetShards.reduce((s, i) => s + (results[i]?.examined || 0), 0);
  const maxLatency = Math.max(...targetShards.map(i => results[i]?.latencyMs || 0));
  const totalLatency = maxLatency + (targetShards.length > 1 ? 2 + Math.floor(Math.random() * 6) : 0);

  // ----- PHASE 7: Mongos → Client (final results) -----
  qfSetPhase(`mongos returns ${allDocs.length} document${allDocs.length !== 1 ? 's' : ''} to client`, 'phase-done');
  qfHighlightLine('qf-line-cm', '#E6EDF3', delay * 1.0);
  await qfPacket('qf-mongos', 'qf-client', '#E6EDF3', delay * 0.85, {
    label: allDocs.length > 0 ? `${allDocs.length}` : '0',
  });
  qfActivate('qf-client', delay * 0.7, '#00ED64');

  qfStep(`returns <strong>${allDocs.length}</strong> document${allDocs.length !== 1 ? 's' : ''} to client · total ${totalLatency} ms`, {
    actor: 'mongos', variant: 'done',
    detail: `examined ${totalExamined} · returned ${allDocs.length} · shards hit ${targetShards.length}/${state.shardCount}`,
  });
  qfStepsDone();

  setExecStatus(`${allDocs.length} doc${allDocs.length !== 1 ? 's' : ''} returned`, 'done');

  // Summary row
  document.getElementById('qe-exec-summary').innerHTML = `
    <div class="qe-sum-item"><span class="qe-sum-label">Returned:</span><span class="qe-sum-val highlight">${allDocs.length}</span></div>
    <div class="qe-sum-item"><span class="qe-sum-label">Examined:</span><span class="qe-sum-val">${totalExamined}</span></div>
    <div class="qe-sum-item"><span class="qe-sum-label">Shards hit:</span><span class="qe-sum-val">${targetShards.length} / ${state.shardCount}</span></div>
    <div class="qe-sum-item"><span class="qe-sum-label">Latency:</span><span class="qe-sum-val">${totalLatency}ms</span></div>
    ${targetShards.length > 1 ? '<div class="qe-sum-item"><span class="qe-sum-label">Phase:</span><span class="qe-sum-val">scatter-gather + merge</span></div>' : ''}
  `;

  // Results panel
  const resultsEl = document.getElementById('qe-results');
  if (allDocs.length === 0) {
    resultsEl.innerHTML = `
      <div class="qe-results-header"><span>Results</span><span style="color:var(--text-muted)">0 documents</span></div>
      <div class="qe-results-empty">No documents matched — insert data first using "Insert Document" or "Insert 50 Docs".</div>
    `;
  } else {
    const preview = allDocs.slice(0, 5);
    const isAgg = queryDef.isAggregate;
    resultsEl.innerHTML = `
      <div class="qe-results-header">
        <span>${isAgg ? 'Aggregation Output' : 'Result Documents'} (first ${preview.length} of ${allDocs.length})</span>
        ${allDocs.length > 5 ? `<span class="qe-more-badge">+${allDocs.length - 5} more</span>` : ''}
      </div>
      ${preview.map(d => formatDoc(d, d._shardIdx)).join('')}
    `;
  }

  if (btn) { btn.disabled = false; btn.innerHTML = '&#9654; Execute'; }
  qeRunning = false;
}

function updateQueryExecutorDisplay() {
  const queries = QUERY_DEFS[state.strategy];
  const selectedId = document.getElementById('qe-select')?.value;
  const queryDef = queries.find(q => q.id === selectedId) || queries[0];
  if (!queryDef) return;

  const codeEl = document.getElementById('qe-code-display');
  const explainEl = document.getElementById('qe-explain-badge');
  const routingBadge = document.getElementById('qe-routing-badge');
  const planTargets = document.getElementById('qe-plan-targets');
  const planExplain = document.getElementById('qe-plan-explain');

  if (codeEl) codeEl.innerHTML = syntaxHighlight(queryDef.code());
  if (explainEl) explainEl.textContent = queryDef.explain?.() || '';

  const typeLabels = { targeted: 'Targeted', scatter: 'Scatter-Gather', partial: 'Partial' };
  const typeClasses = { targeted: 'qe-badge-targeted', scatter: 'qe-badge-scatter', partial: 'qe-badge-partial' };
  if (routingBadge) {
    routingBadge.textContent = typeLabels[queryDef.type] || queryDef.type;
    routingBadge.className = `qe-routing-badge ${typeClasses[queryDef.type] || ''}`;
  }

  const COLORS = ['#3B82F6', '#A855F7', '#F59E0B', '#EF4444'];
  const targetShards = queryDef.getTargetShards();
  const shards = getShards();
  if (planTargets) {
    planTargets.innerHTML = '<span class="qe-plan-arrow">→</span>' + targetShards.map(i => {
      const s = shards[i];
      if (!s) return '';
      return `<span class="qe-plan-shard" style="background:${COLORS[i % 4]}22;color:${COLORS[i % 4]}">${s.rsName}</span>`;
    }).join('');
  }
  if (planExplain) planExplain.textContent = queryDef.explanation;

  // Reset execution rows for new query
  shards.forEach((_, i) => setExecRowState(i, 'state-idle', 'idle'));
  const summary = document.getElementById('qe-exec-summary');
  const results = document.getElementById('qe-results');
  if (summary) summary.innerHTML = '';
  if (results) results.innerHTML = '<div class="qe-results-empty">Select a query above and click Execute to run it across shards.</div>';
  setExecStatus('Ready', '');
  qfResetAll();
  qfSetPhase('Idle — click Execute to animate the query flow.', '');
  qfStepsIdle();
}

// ===== QUERY FLOW DIAGRAM =====

function renderFlowDiagram(shards, COLORS) {
  const shardCount = shards.length;
  const W = 860;
  const H = Math.max(280, 140 + shardCount * 56);

  // Node layout (px)
  const CLIENT_X = 30,  CLIENT_W  = 120, CLIENT_H  = 60;
  const MONGOS_X = 330, MONGOS_W  = 150, MONGOS_H  = 70;
  const CONFIG_X = 342, CONFIG_W  = 130, CONFIG_H  = 50;
  const SHARD_X  = 680, SHARD_W   = 150, SHARD_H   = 50;

  const clientY = (H - CLIENT_H) / 2;
  const mongosY = (H - MONGOS_H) / 2;
  const configY = 12;

  const usableH = H - 40;
  const shardGap = usableH / (shardCount + 1);
  const shardNodes = shards.map((s, i) => ({
    idx: i,
    x: SHARD_X,
    y: 20 + shardGap * (i + 1) - SHARD_H / 2,
    w: SHARD_W,
    h: SHARD_H,
    shard: s,
  }));

  // Connector coordinates (center-to-edge)
  const cmX1 = CLIENT_X + CLIENT_W, cmY1 = clientY + CLIENT_H / 2;
  const cmX2 = MONGOS_X,            cmY2 = mongosY + MONGOS_H / 2;
  const mcX  = MONGOS_X + MONGOS_W / 2;
  const mcY1 = mongosY, mcY2 = configY + CONFIG_H;

  const svgLines = [
    `<line class="qf-line" id="qf-line-cm" x1="${cmX1}" y1="${cmY1}" x2="${cmX2}" y2="${cmY2}" />`,
    `<line class="qf-line qf-line-dashed" id="qf-line-mc" x1="${mcX}" y1="${mcY1}" x2="${mcX}" y2="${mcY2}" />`,
    ...shardNodes.map(n => {
      const sx = MONGOS_X + MONGOS_W;
      const sy = mongosY + MONGOS_H / 2;
      const ex = n.x;
      const ey = n.y + n.h / 2;
      return `<line class="qf-line qf-line-shard" id="qf-line-shard-${n.idx}" data-shard="${n.idx}" x1="${sx}" y1="${sy}" x2="${ex}" y2="${ey}" />`;
    }),
  ].join('');

  const clientNode = `
    <div class="qf-node qf-client" id="qf-client" style="left:${CLIENT_X}px;top:${clientY}px;width:${CLIENT_W}px;height:${CLIENT_H}px">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8M12 17v4"/>
      </svg>
      <div class="qf-node-label">Client / App</div>
      <div class="qf-node-sub">driver</div>
    </div>`;

  const mongosNode = `
    <div class="qf-node qf-mongos" id="qf-mongos" style="left:${MONGOS_X}px;top:${mongosY}px;width:${MONGOS_W}px;height:${MONGOS_H}px">
      <div class="qf-node-title">mongos</div>
      <div class="qf-node-label">Query Router</div>
      <div class="qf-node-sub">stateless · routes &amp; merges</div>
    </div>`;

  const configNode = `
    <div class="qf-node qf-config" id="qf-config" style="left:${CONFIG_X}px;top:${configY}px;width:${CONFIG_W}px;height:${CONFIG_H}px">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01z"/>
      </svg>
      <div class="qf-node-label">Config Server</div>
      <div class="qf-node-sub">chunk map</div>
    </div>`;

  const shardNodesHTML = shardNodes.map(n => {
    const color = COLORS[n.idx % COLORS.length];
    return `
      <div class="qf-node qf-shard" id="qf-shard-${n.idx}" data-shard="${n.idx}"
           style="left:${n.x}px;top:${n.y}px;width:${n.w}px;height:${n.h}px;--shard-color:${color}">
        <div class="qf-shard-dot" style="background:${color}"></div>
        <div class="qf-shard-body">
          <div class="qf-node-label">${n.shard.rsName}</div>
          <div class="qf-node-sub">${n.shard.docCount} docs</div>
        </div>
      </div>`;
  }).join('');

  const legend = `
    <div class="qf-legend">
      <span class="qf-legend-item"><span class="qf-swatch" style="background:#00ED64"></span>query</span>
      <span class="qf-legend-item"><span class="qf-swatch" style="background:#F59E0B"></span>metadata</span>
      <span class="qf-legend-item"><span class="qf-swatch qf-swatch-multi"></span>per-shard payload</span>
      <span class="qf-legend-item"><span class="qf-swatch" style="background:#E6EDF3"></span>merged results</span>
    </div>`;

  const phaseLabel = `<div class="qf-phase" id="qf-phase"><span class="qf-phase-dot"></span><span id="qf-phase-text">Idle — click Execute to animate the query flow.</span></div>`;

  const stepsPanel = `
    <div class="qf-steps-panel" id="qf-steps-panel">
      <div class="qf-steps-header">
        <span>Execution Steps</span>
        <span class="qf-steps-timer" id="qf-steps-timer">—</span>
      </div>
      <ol class="qf-steps-list" id="qf-steps-list">
        <li class="qf-steps-empty">Click <strong>Execute</strong> to see the step-by-step flow through mongos, config server, and shards.</li>
      </ol>
    </div>`;

  return `
    <div class="qe-flow-wrap">
      <div class="qe-flow-header">
        <span>Query Flow</span>
        ${legend}
      </div>
      <div class="qe-flow-scroll">
        <div class="qe-flow" id="qe-flow" style="width:${W}px;height:${H}px">
          <svg class="qe-flow-svg" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none">
            <defs>
              <marker id="qf-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                <path d="M0 0 L10 5 L0 10 z" fill="currentColor"/>
              </marker>
            </defs>
            ${svgLines}
          </svg>
          ${clientNode}
          ${configNode}
          ${mongosNode}
          ${shardNodesHTML}
        </div>
      </div>
      ${phaseLabel}
      ${stepsPanel}
    </div>`;
}

function qfSetPhase(text, cls) {
  const el = document.getElementById('qf-phase');
  const txt = document.getElementById('qf-phase-text');
  if (txt) txt.textContent = text;
  if (el) el.className = `qf-phase ${cls || ''}`;
}

function qfActivate(nodeId, durationMs, colorHint) {
  const el = document.getElementById(nodeId);
  if (!el) return;
  el.classList.add('qf-active');
  if (colorHint) el.style.setProperty('--qf-pulse', colorHint);
  setTimeout(() => {
    el.classList.remove('qf-active');
    el.style.removeProperty('--qf-pulse');
  }, durationMs);
}

function qfHighlightLine(lineId, color, durationMs) {
  const el = document.getElementById(lineId);
  if (!el) return;
  el.classList.add('qf-line-active');
  if (color) el.style.stroke = color;
  setTimeout(() => {
    el.classList.remove('qf-line-active');
    el.style.stroke = '';
  }, durationMs);
}

function qfPacket(fromId, toId, color, durationMs, opts = {}) {
  return new Promise(resolve => {
    const container = document.getElementById('qe-flow');
    const from = document.getElementById(fromId);
    const to = document.getElementById(toId);
    if (!container || !from || !to) { setTimeout(resolve, durationMs); return; }

    const cRect = container.getBoundingClientRect();
    const fRect = from.getBoundingClientRect();
    const tRect = to.getBoundingClientRect();

    const sx = fRect.left - cRect.left + fRect.width / 2;
    const sy = fRect.top - cRect.top + fRect.height / 2;
    const ex = tRect.left - cRect.left + tRect.width / 2;
    const ey = tRect.top - cRect.top + tRect.height / 2;

    const p = document.createElement('div');
    p.className = 'qf-packet';
    if (opts.label) {
      p.textContent = opts.label;
      p.classList.add('qf-packet-labeled');
    }
    p.style.background = color;
    p.style.boxShadow = `0 0 10px ${color}`;
    p.style.left = `${sx}px`;
    p.style.top = `${sy}px`;
    container.appendChild(p);

    // Force a reflow so the transition engages
    void p.offsetWidth;
    p.style.transition = `transform ${durationMs}ms cubic-bezier(0.4, 0, 0.2, 1), opacity ${durationMs}ms`;
    p.style.transform = `translate(${ex - sx}px, ${ey - sy}px)`;

    setTimeout(() => { p.style.opacity = '0'; }, Math.max(0, durationMs - 80));
    setTimeout(() => { p.remove(); resolve(); }, durationMs + 40);
  });
}

function qfResetAll() {
  document.querySelectorAll('#qe-flow .qf-packet').forEach(n => n.remove());
  document.querySelectorAll('#qe-flow .qf-node').forEach(n => {
    n.classList.remove('qf-active', 'qf-dim');
    n.style.removeProperty('--qf-pulse');
  });
  document.querySelectorAll('#qe-flow .qf-line').forEach(n => {
    n.classList.remove('qf-line-active');
    n.style.stroke = '';
  });
}

// ----- Step-by-step execution log -----
let qfStepSeq = 0;
let qfRunStart = 0;

function qfStepsReset() {
  qfStepSeq = 0;
  qfRunStart = performance.now();
  const list = document.getElementById('qf-steps-list');
  const timer = document.getElementById('qf-steps-timer');
  if (list) list.innerHTML = '';
  if (timer) timer.textContent = '0 ms';
}

function qfStepsIdle() {
  qfStepSeq = 0;
  qfRunStart = 0;
  const list = document.getElementById('qf-steps-list');
  const timer = document.getElementById('qf-steps-timer');
  if (list) list.innerHTML = `<li class="qf-steps-empty">Click <strong>Execute</strong> to see the step-by-step flow through mongos, config server, and shards.</li>`;
  if (timer) timer.textContent = '—';
}

function qfStep(text, opts = {}) {
  const list = document.getElementById('qf-steps-list');
  const timer = document.getElementById('qf-steps-timer');
  if (!list) return;
  qfStepSeq++;
  const elapsed = qfRunStart ? Math.round(performance.now() - qfRunStart) : 0;
  if (timer) timer.textContent = `${elapsed} ms`;

  const li = document.createElement('li');
  li.className = `qf-step ${opts.variant ? 'qf-step-' + opts.variant : ''}`;
  const actor = opts.actor ? `<span class="qf-step-actor" style="${opts.actorColor ? `color:${opts.actorColor}` : ''}">${opts.actor}</span>` : '';
  const detail = opts.detail ? `<span class="qf-step-detail">${opts.detail}</span>` : '';
  li.innerHTML = `
    <span class="qf-step-num">${qfStepSeq}</span>
    <div class="qf-step-body">
      <div class="qf-step-text">${actor}<span class="qf-step-action">${text}</span></div>
      ${detail}
    </div>
    <span class="qf-step-time">+${elapsed}ms</span>`;
  list.appendChild(li);
  list.scrollTop = list.scrollHeight;
}

function qfStepsDone() {
  const timer = document.getElementById('qf-steps-timer');
  if (timer && qfRunStart) {
    const total = Math.round(performance.now() - qfRunStart);
    timer.textContent = `done · ${total} ms`;
  }
}

// ===== QUERY ROUTING VIEW =====

function renderQueryRouting() {
  const s = state.strategy;
  const key = state.shardKey;
  const shards = getShards();
  const COLORS = ['#3B82F6', '#A855F7', '#F59E0B', '#EF4444'];
  const queries = QUERY_DEFS[s];
  const firstQuery = queries[0];
  const targetShards = firstQuery.getTargetShards();
  const typeLabels = { targeted: 'Targeted', scatter: 'Scatter-Gather', partial: 'Partial' };
  const typeClasses = { targeted: 'qe-badge-targeted', scatter: 'qe-badge-scatter', partial: 'qe-badge-partial' };

  // Build select options — use the friendly query name; aggregation pipelines are
  // unreadable when condensed to a single line, so show q.name instead.
  const TYPE_ICONS = { targeted: '●', scatter: '◎', partial: '◑' };
  const options = queries.map(q => {
    return `<option value="${q.id}">${TYPE_ICONS[q.type] || '○'} ${q.name}</option>`;
  }).join('');

  // Build exec rows for current shards
  const execRowsHTML = renderExecRows(shards);

  // Target shard badges for plan row
  const planBadges = targetShards.map(i => {
    const sh = shards[i];
    if (!sh) return '';
    return `<span class="qe-plan-shard" style="background:${COLORS[i%4]}22;color:${COLORS[i%4]}">${sh.rsName}</span>`;
  }).join('');

  // Static reference cards
  const refQueries = [
    {
      name: 'Targeted (Best)',
      efficiency: 'targeted',
      explanation: 'Query includes the shard key or its prefix — mongos routes to exactly 1 shard. Zero scatter overhead.',
      example: s === 'range' ? `find({ ${key}: "exact_value" })` : s === 'hashed' ? `find({ ${key}: "exact_value" })` : `find({ region: "europe" })`,
    },
    {
      name: 'Partial (Good)',
      efficiency: 'partial',
      explanation: 'Query hits a predictable subset of shards — e.g. $in on shard key (hashed), or range spanning N chunks (range), or zone $in (zone).',
      example: s === 'range' ? `find({ ${key}: { $gte: A, $lte: B } })` : s === 'hashed' ? `find({ ${key}: { $in: [v1, v2] } })` : `find({ region: { $in: ["americas","europe"] } })`,
    },
    {
      name: 'Scatter-Gather (Avoid)',
      efficiency: 'scatter',
      explanation: 'No shard key in the predicate — mongos broadcasts to ALL shards, each scans independently, mongos merges. Expensive at scale.',
      example: s === 'range' ? `find({ status: "shipped" })` : s === 'hashed' ? `find({ country: "US" })` : `find({ tier: "enterprise" })`,
    },
  ];

  const refCards = refQueries.map(q => {
    const effClass = { targeted: 'eff-targeted', scatter: 'eff-scatter', partial: 'eff-partial' }[q.efficiency];
    return `
      <div class="query-card">
        <div class="query-header">
          <div class="query-name">${q.name}</div>
          <div class="query-efficiency ${effClass}">${q.name}</div>
        </div>
        <div class="query-body">
          <div class="query-code">${q.example}</div>
          <div class="query-explanation">${q.explanation}</div>
        </div>
      </div>`;
  }).join('');

  document.getElementById('routing-panel').innerHTML = `
    <div class="routing-title">Query Executor</div>
    <div class="routing-subtitle" style="margin-bottom:16px;">
      Run queries against your <strong>${STRATEGIES[s].name}</strong> cluster
      (key: <code style="font-family:monospace;color:var(--mongo-green)">${key}</code>).
      Results use your inserted documents.
    </div>

    <div class="query-executor">
      <!-- Toolbar -->
      <div class="qe-toolbar">
        <div class="qe-toolbar-left">
          <span class="qe-label">Query</span>
          <select id="qe-select" class="qe-select">${options}</select>
        </div>
        <div class="qe-speed-control" data-tooltip="<strong>Animation Speed</strong><br>Controls how fast packets travel between client, mongos, config server, and shards. Does not affect real query semantics.">
          <span class="qe-label">Speed</span>
          <input type="range" id="qe-speed-slider" class="qe-speed-slider" min="1" max="5" step="1" value="${state.routingSpeed ?? state.animationSpeed}">
          <span class="qe-speed-label" id="qe-speed-label">${['Very Slow','Slow','Normal','Fast','Very Fast'][(state.routingSpeed ?? state.animationSpeed) - 1] || 'Normal'}</span>
        </div>
        <button id="qe-run-btn" class="btn-primary qe-run-btn">&#9654; Execute</button>
      </div>

      <!-- Code display -->
      <div class="qe-code-wrap">
        <div class="qe-code-header">
          <span>MongoDB Shell</span>
          <span class="qe-explain-badge" id="qe-explain-badge">${firstQuery.explain?.() || ''}</span>
        </div>
        <pre class="qe-code" id="qe-code-display">${syntaxHighlight(firstQuery.code())}</pre>
      </div>

      <!-- Routing plan -->
      <div class="qe-plan-row">
        <span class="qe-plan-label">Routing:</span>
        <span class="qe-routing-badge ${typeClasses[firstQuery.type]}" id="qe-routing-badge">${typeLabels[firstQuery.type]}</span>
        <div class="qe-plan-targets" id="qe-plan-targets">
          <span class="qe-plan-arrow">→</span>${planBadges}
        </div>
        <span class="qe-plan-explain" id="qe-plan-explain">${firstQuery.explanation}</span>
      </div>

      <!-- Visual flow diagram -->
      ${renderFlowDiagram(shards, COLORS)}

      <!-- Execution table -->
      <div class="qe-exec-panel">
        <div class="qe-exec-header">
          <span>Execution</span>
          <span class="qe-exec-status" id="qe-exec-status">Ready</span>
        </div>
        <div class="qe-exec-col-header">
          <span>Shard</span><span>Status</span><span>Progress</span>
          <span>Returned</span><span>Examined</span><span>Latency</span>
        </div>
        <div id="qe-exec-rows">${execRowsHTML}</div>
        <div class="qe-exec-summary" id="qe-exec-summary"></div>
      </div>

      <!-- Results -->
      <div class="qe-results" id="qe-results">
        <div class="qe-results-empty">Select a query and click Execute to run it across shards.</div>
      </div>
    </div>

    <div class="qe-section-divider">Query Pattern Reference</div>
    <div class="query-examples">${refCards}</div>
  `;
}

// ===== LOG =====

function addLog(type, msg) {
  const el = document.getElementById('log-entries');
  const now = new Date().toLocaleTimeString('en-US', { hour12: false });
  const div = document.createElement('div');
  div.className = `log-entry log-${type}`;
  div.innerHTML = `<span class="log-time">${now}</span><span class="log-msg">${msg}</span>`;
  el.insertBefore(div, el.firstChild);
  // Keep last 80 entries
  while (el.children.length > 80) el.removeChild(el.lastChild);
}

// ===== INSERT SIMULATION =====

async function insertOne() {
  if (state.isInserting) return;
  state.isInserting = true;

  const doc = generateDocument();
  const shardIdx = routeDocument(doc);
  doc._shardIdx = shardIdx;
  state.documents.push(doc);
  state.totalInserted++;

  const shardCard = document.getElementById(`shard-card-${shardIdx}`);
  if (shardCard) {
    shardCard.classList.add('active-routing');
    shardCard.classList.add('inserting');
    setTimeout(() => {
      shardCard.classList.remove('active-routing');
      shardCard.classList.remove('inserting');
    }, speedMs() * 2);
  }

  // Update shard counts without full re-render for speed
  updateShardCounts();

  const keyVal = getDocKeyValue(doc);
  const rsName = getShards()[shardIdx]?.rsName || `shard${shardIdx + 1}`;
  addLog('route', `INSERT → <strong>${rsName}</strong> | ${state.shardKey}: "${keyVal}" | _id: ${doc._id.slice(-8)}`);

  updateHeaderStats();

  await sleep(speedMs());
  state.isInserting = false;
}

async function insertBatch(count = 50) {
  const batchSpeedMs = Math.max(20, speedMs() / 3);
  addLog('info', `Batch inserting ${count} documents…`);
  for (let i = 0; i < count; i++) {
    const doc = generateDocument();
    const shardIdx = routeDocument(doc);
    doc._shardIdx = shardIdx;
    state.documents.push(doc);
    state.totalInserted++;

    const shardCard = document.getElementById(`shard-card-${shardIdx}`);
    if (shardCard) {
      shardCard.classList.add('active-routing');
      setTimeout(() => shardCard.classList.remove('active-routing'), batchSpeedMs * 1.5);
    }
    updateShardCounts();
    updateHeaderStats();

    if (i % 5 === 0) await sleep(batchSpeedMs);
  }
  addLog('info', `Batch complete. Total: <strong>${state.documents.length}</strong> docs across <strong>${state.shardCount}</strong> shards.`);
  renderDistribution();
  renderQueryRouting();
}

async function insertBulk(count = 10000) {
  if (state.isBulkLoading) return;
  state.isBulkLoading = true;

  const btn = document.getElementById('btn-insert-bulk');
  const wrap = document.getElementById('bulk-progress-wrap');
  const bar = document.getElementById('bulk-progress-bar');
  const label = document.getElementById('bulk-progress-label');
  if (btn) { btn.disabled = true; btn.innerHTML = '⏳ Loading…'; }
  if (wrap) wrap.style.display = 'flex';

  const CHUNK = 500;   // insert in chunks to yield to the browser between batches
  const t0 = Date.now();

  for (let i = 0; i < count; i++) {
    const doc = generateDocument();
    const shardIdx = routeDocument(doc);
    doc._shardIdx = shardIdx;
    state.documents.push(doc);
    state.totalInserted++;

    // Yield to browser every CHUNK docs to keep the UI responsive
    if ((i + 1) % CHUNK === 0) {
      const pct = ((i + 1) / count) * 100;
      if (bar) bar.style.width = `${pct}%`;
      if (label) label.textContent = `${(i + 1).toLocaleString()} / ${count.toLocaleString()}`;
      updateShardCounts();
      updateHeaderStats();
      await sleep(0); // yield
    }
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);

  // Final UI update
  if (bar) bar.style.width = '100%';
  if (label) label.textContent = `${count.toLocaleString()} / ${count.toLocaleString()}`;
  updateShardCounts();
  updateHeaderStats();
  renderDistribution();
  renderQueryRouting();

  // Show balance stats in log
  const shards = getShards();
  const counts = shards.map(s => s.docCount);
  const avg = Math.round(state.documents.length / shards.length);
  const maxDev = Math.max(...counts.map(c => Math.abs(c - avg)));
  const balance = (100 - (maxDev / avg) * 100).toFixed(1);
  addLog('info',
    `Bulk load done: <strong>${count.toLocaleString()} docs</strong> in ${elapsed}s — ` +
    `distribution: [${counts.join(', ')}] — balance: <strong>${balance}%</strong>`
  );

  setTimeout(() => {
    if (wrap) wrap.style.display = 'none';
    if (bar) bar.style.width = '0';
  }, 2000);

  if (btn) { btn.disabled = false; btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 7h16M4 12h16M4 17h16"/></svg> Load 10,000 Docs'; }
  state.isBulkLoading = false;
}

function updateShardCounts() {
  const shards = getShards();
  const totalDocs = state.documents.length;
  shards.forEach(s => {
    const countEl = document.getElementById(`doc-count-${s.index}`);
    const fillEl = document.getElementById(`fill-${s.index}`);
    if (countEl) {
      countEl.textContent = `${s.docCount} docs`;
      countEl.classList.add('updated');
      setTimeout(() => countEl.classList.remove('updated'), 600);
    }
    if (fillEl) {
      const pct = totalDocs > 0 ? (s.docCount / totalDocs) * 100 : 0;
      fillEl.style.width = `${pct}%`;
    }
  });
}

function updateHeaderStats() {
  const docsEl   = document.getElementById('header-docs');
  const shardsEl = document.getElementById('header-shards');
  const activeTab = document.querySelector('.view-tab.active')?.dataset.tab ?? 'topology';

  if (activeTab === 'balancer') {
    const totalMB  = bSt.chunks.reduce((s, c) => s + c.sizeMB, 0);
    const shards   = bSt.shards.length;
    docsEl.textContent   = `${totalMB >= 1000 ? (totalMB / 1000).toFixed(2) + ' GB' : totalMB.toFixed(0) + ' MB'} data`;
    shardsEl.textContent = `${shards} shard${shards !== 1 ? 's' : ''}`;
  } else if (activeTab === 'automerger') {
    const totalMB = aSt.chunks.reduce((s, c) => s + c.sizeMB, 0);
    docsEl.textContent   = `${aSt.chunks.length} chunks`;
    shardsEl.textContent = `${aSt.shardCount} shards · ${totalMB.toFixed(0)} MB`;
  } else {
    docsEl.textContent   = `${state.documents.length} docs`;
    shardsEl.textContent = `${state.shardCount} shards`;
  }

  docsEl.classList.add('updated');
  setTimeout(() => docsEl.classList.remove('updated'), 400);
}

function getDocKeyValue(doc) {
  const k = state.shardKey;
  if (!k) return '?';
  if (k === 'region' || k === 'country') {
    return doc.region || doc.country || '?';
  }
  return String(doc[k] || doc._id).slice(0, 16);
}

function speedMs() {
  // speed 1=slow(600ms) … 5=fast(60ms)
  return Math.round(600 / (state.animationSpeed * 1.5));
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// ===== FULL RENDER =====

function renderAll() {
  renderStrategyInfo();
  renderShardKeys();
  renderRegionDetail();
  renderTopology();
  renderDistribution();
  renderQueryRouting();
  updateHeaderStats();
}

// ===== EVENT LISTENERS =====

function initEventListeners() {
  // Strategy
  document.querySelectorAll('.strategy-card').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.strategy-card').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      state.strategy = btn.dataset.strategy;
      state.shardKey = null;
      state.documents = [];
      renderAll();
      addLog('info', `Strategy switched to <strong>${STRATEGIES[state.strategy].name}</strong>.`);
    });
  });

  // Region mode
  document.querySelectorAll('.toggle-btn[data-region]').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.toggle-btn[data-region]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      state.regionMode = btn.dataset.region;
      state.documents = [];
      renderAll();
      addLog('info', `Region mode: <strong>${state.regionMode === 'multi' ? 'Multi-Region' : 'Single Region'}</strong>. Data cleared.`);
    });
  });

  // Shard count
  document.getElementById('shard-count-group').addEventListener('click', e => {
    const btn = e.target.closest('.count-btn');
    if (!btn) return;
    document.querySelectorAll('#shard-count-group .count-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    state.shardCount = parseInt(btn.dataset.count);
    state.documents = [];
    renderAll();
    addLog('info', `Cluster resized to <strong>${state.shardCount} shards</strong>. Data cleared.`);
  });

  // Nodes per shard
  document.getElementById('nodes-per-shard-group').addEventListener('click', e => {
    const btn = e.target.closest('.count-btn');
    if (!btn) return;
    document.querySelectorAll('#nodes-per-shard-group .count-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    state.nodesPerShard = parseInt(btn.dataset.nodes);
    renderTopology();
    addLog('info', `Nodes per shard: <strong>${state.nodesPerShard}</strong>. Topology updated.`);
  });

  // Mongos count
  document.getElementById('mongos-count-group').addEventListener('click', e => {
    const btn = e.target.closest('.count-btn');
    if (!btn) return;
    document.querySelectorAll('#mongos-count-group .count-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    state.mongosCount = parseInt(btn.dataset.mongos);
    renderTopology();
    addLog('info', `Mongos routers: <strong>${state.mongosCount}</strong>.`);
  });

  // Insert
  document.getElementById('btn-insert-one').addEventListener('click', () => insertOne());
  document.getElementById('btn-insert-batch').addEventListener('click', () => insertBatch(50));
  document.getElementById('btn-insert-bulk').addEventListener('click', () => insertBulk(10000));
  document.getElementById('btn-clear').addEventListener('click', () => {
    state.documents = [];
    docSeq = 0;
    renderTopology();
    renderDistribution();
    renderQueryRouting();
    updateHeaderStats();
    addLog('warn', 'All documents cleared.');
  });

  // Reset all
  document.getElementById('btn-reset-all').addEventListener('click', () => {
    state.strategy = 'range';
    state.shardKey = null;
    state.regionMode = 'single';
    state.shardCount = 2;
    state.nodesPerShard = 3;
    state.mongosCount = 1;
    state.documents = [];
    state.isBulkLoading = false;
    docSeq = 0;
    const bpw = document.getElementById('bulk-progress-wrap');
    if (bpw) bpw.style.display = 'none';
    // Reset UI toggles
    document.querySelectorAll('.strategy-card').forEach(b => b.classList.remove('active'));
    document.querySelector('.strategy-card[data-strategy="range"]').classList.add('active');
    document.querySelectorAll('.toggle-btn[data-region]').forEach(b => b.classList.remove('active'));
    document.querySelector('.toggle-btn[data-region="single"]').classList.add('active');
    resetCountBtns('shard-count-group', '2');
    resetCountBtns('nodes-per-shard-group', '3');
    resetCountBtns('mongos-count-group', '1');
    renderAll();
    document.getElementById('log-entries').innerHTML = '<div class="log-entry log-info"><span class="log-msg">Reset to defaults.</span></div>';
    addLog('info', 'Cluster reset to defaults.');
  });

  function resetCountBtns(groupId, val) {
    document.querySelectorAll(`#${groupId} .count-btn`).forEach(b => {
      b.classList.toggle('active', b.dataset.count === val || b.dataset.nodes === val || b.dataset.mongos === val);
    });
  }

  // Speed slider
  const slider = document.getElementById('speed-slider');
  const speedLabels = ['', 'Slow', 'Normal', 'Normal', 'Fast', 'Instant'];
  slider.addEventListener('input', () => {
    state.animationSpeed = parseInt(slider.value);
    document.getElementById('speed-label').textContent = speedLabels[state.animationSpeed] || 'Normal';
  });

  // View tabs
  document.querySelectorAll('.view-tab').forEach(tab => {
    tab.addEventListener('click', () => {
      document.querySelectorAll('.view-tab').forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
      tab.classList.add('active');
      document.getElementById(`tab-${tab.dataset.tab}`).classList.add('active');

      // Hide irrelevant sidebar sections when balancer or limitations tab is active
      const isBalancer = tab.dataset.tab === 'balancer';
      const isAutoMerger = tab.dataset.tab === 'automerger';
      const isLimitations = tab.dataset.tab === 'limitations';
      const hideSidebar = isBalancer || isAutoMerger || isLimitations;
      document.querySelectorAll('.strategy-section, .shardkey-section, .region-section, .cluster-section, .datasim-section')
        .forEach(s => s.classList.toggle('hidden-section', hideSidebar));

      updateHeaderStats();

      // Lazy-render balancer tab on first open
      if (isBalancer) {
        bInitState(bSt.shardCount, bSt.chunksPerShard);
        renderBalancer();
        bLog('info', `Balancer tab opened — ${bSt.shardCount} shards × ${bSt.chunksPerShard} chunks, threshold: ${bThreshold()} MB (3 × ${bSt.chunkSizeMB} MB chunkSize).`);
      }

      // Lazy-render AutoMerger tab on first open
      if (isAutoMerger) {
        if (!aSt.initialized) aInitState();
        renderAutoMerger();
        aLog('info', 'AutoMerger tab opened. You can merge adjacent chunks and then run the same-tab balancer to resolve hot-shard skew.');
      }
    });
  });

  // Clear log
  document.getElementById('btn-clear-log').addEventListener('click', () => {
    document.getElementById('log-entries').innerHTML = '';
  });

  // Query executor — select change updates code + plan display
  document.getElementById('routing-panel').addEventListener('change', e => {
    if (e.target.id === 'qe-select') updateQueryExecutorDisplay();
  });

  // Query executor — execute button (delegated since panel re-renders)
  document.getElementById('routing-panel').addEventListener('click', e => {
    if (e.target.closest('#qe-run-btn')) executeQueryAnimation();
  });

  // Query executor — speed slider (delegated so it survives panel re-renders)
  document.getElementById('routing-panel').addEventListener('input', e => {
    if (e.target.id !== 'qe-speed-slider') return;
    const v = parseInt(e.target.value, 10);
    state.routingSpeed = v;
    const labelEl = document.getElementById('qe-speed-label');
    if (labelEl) {
      labelEl.textContent = ['Very Slow','Slow','Normal','Fast','Very Fast'][v - 1] || 'Normal';
    }
  });

  // Keyboard shortcut: Space to insert; Enter when routing tab active to execute query
  document.addEventListener('keydown', e => {
    if (e.target.matches('input, button, select, textarea')) return;
    if (e.code === 'Space') { e.preventDefault(); insertOne(); }
    if (e.code === 'Enter') {
      const routingTabActive = document.getElementById('tab-routing')?.classList.contains('active');
      if (routingTabActive) { e.preventDefault(); executeQueryAnimation(); }
    }
  });
}

// ===================================================================
// ===== LOAD BALANCER MODULE =========================================
// ===================================================================

// Module-level balancer state (independent of main sharding state)
const bSt = {
  shards: [],       // [{ id, name }]
  chunks: [],       // [{ id, shardIdx, sizeMB, docs, isJumbo, justArrived }]
  shardCount: 3,
  chunksPerShard: 8,
  chunkSizeMB: 128, // MongoDB default chunkSize
  thresholdMB: 384, // 3 × chunkSize — MongoDB's data-size balancing threshold
  running: false,
  animating: false,
  showJumbo: false,
  migrations: 0,
  log: [],
  speed: 2,
};
let bChunkSeq = 0;

// ----- helpers -----

function bNewChunk(shardIdx, sizeMB, isJumbo = false) {
  bChunkSeq++;
  return {
    id: `ch_${String(bChunkSeq).padStart(3, '0')}`,
    shardIdx,
    sizeMB: +sizeMB.toFixed(1),
    docs: Math.floor(sizeMB * 80 + Math.random() * 200),
    isJumbo,
    justArrived: false,
  };
}

function bInitState(shardCount, chunksPerShard) {
  bChunkSeq = 0;
  bSt.shardCount  = shardCount;
  bSt.chunksPerShard = chunksPerShard;
  bSt.migrations  = 0;
  bSt.log         = [];
  bSt.running     = false;
  bSt.animating   = false;
  bSt.shards = Array.from({ length: shardCount }, (_, i) => ({ id: i, name: `rs${i + 1}` }));
  bSt.chunks = [];
  for (let s = 0; s < shardCount; s++) {
    for (let c = 0; c < chunksPerShard; c++) {
      bSt.chunks.push(bNewChunk(s, 20 + Math.random() * 80)); // 20–100 MB
    }
  }
}

function bChunksOf(shardIdx) { return bSt.chunks.filter(c => c.shardIdx === shardIdx); }

// MongoDB 6.0+ balancer threshold: data-size difference in MB
function bThreshold() { return bSt.thresholdMB; }

function bGetBalance() {
  const sizes   = bSt.shards.map(s => +(bChunksOf(s.id).reduce((sum, c) => sum + c.sizeMB, 0).toFixed(1)));
  const counts  = bSt.shards.map(s => bChunksOf(s.id).length);
  const maxSize  = Math.max(...sizes);
  const minSize  = Math.min(...sizes);
  const maxIdx   = sizes.indexOf(maxSize);
  const minIdx   = sizes.indexOf(minSize);
  const avgSize  = +(sizes.reduce((a, b) => a + b, 0) / sizes.length).toFixed(1);
  const threshold = bThreshold();
  const diff      = +(maxSize - minSize).toFixed(1);
  return { sizes, counts, maxSize, minSize, maxIdx, minIdx, avgSize, threshold, diff,
           isBalanced: diff <= threshold };
}

function bLog(type, msg) {
  const now = new Date().toLocaleTimeString('en-US', { hour12: false });
  bSt.log.unshift({ type, msg, time: now });
  if (bSt.log.length > 60) bSt.log.pop();
  // Refresh just the log list if the panel is visible
  const el = document.getElementById('bal-log-entries');
  if (el) el.innerHTML = bRenderLogHTML();
}

function bSpeedMs() { return Math.max(150, 1200 - bSt.speed * 200); }

// ----- chunk colour (by size) -----
function bChunkClass(chunk) {
  if (chunk.isJumbo) return 'jumbo';
  if (chunk.sizeMB > 90) return 'large';
  if (chunk.sizeMB > 55) return 'medium';
  return 'normal';
}

// ----- rendering helpers -----

function bRenderChunkHTML(chunk) {
  const cls = bChunkClass(chunk);
  const arrived = chunk.justArrived ? ' just-arrived' : '';
  const jumboIcon = chunk.isJumbo ? '<span class="jumbo-icon">⚠</span>' : '';
  return `
    <div class="bal-chunk ${cls}${arrived}" id="bch-${chunk.id}" data-chunk-id="${chunk.id}">
      <span class="bal-chunk-id">${jumboIcon}${chunk.id}</span>
      <span class="bal-chunk-size">${chunk.sizeMB}MB</span>
    </div>`;
}

function bRenderShardHTML(shard, bal) {
  const chunks  = bChunksOf(shard.id);
  const count   = chunks.length;
  const isMax   = shard.id === bal.maxIdx && bal.diff > bal.threshold;
  const isMin   = shard.id === bal.minIdx && bal.diff > bal.threshold;
  const sizeCls  = isMax ? 'most' : isMin ? 'least' : 'mid';
  const COLORS  = ['#3B82F6', '#A855F7', '#F59E0B', '#EF4444'];
  const color   = COLORS[shard.id % 4];
  const totalMB = chunks.reduce((s, c) => s + c.sizeMB, 0).toFixed(0);
  return `
    <div class="bal-shard" id="bal-shard-${shard.id}">
      <div class="bal-shard-header">
        <span class="bal-shard-name" style="color:${color}">${shard.name}</span>
        <span class="bal-shard-count ${sizeCls}">${totalMB} MB</span>
      </div>
      <div class="bal-chunks" id="bal-chunks-${shard.id}">
        ${chunks.map(bRenderChunkHTML).join('')}
      </div>
      <div class="bal-shard-footer">${count} chunks · ${totalMB} MB total</div>
    </div>`;
}

function bRenderDistBarHTML(bal) {
  const max = Math.max(...bal.sizes, 1);
  const COLORS = ['#3B82F6', '#A855F7', '#F59E0B', '#EF4444'];
  const rows = bSt.shards.map((s, i) => {
    const pct = (bal.sizes[i] / max) * 100;
    return `
      <div class="bal-dist-row">
        <span class="bal-dist-name" style="color:${COLORS[i % 4]}">${s.name}</span>
        <div class="bal-dist-track">
          <div class="bal-dist-fill" style="width:${pct}%;background:${COLORS[i % 4]}"></div>
        </div>
        <span class="bal-dist-num" style="color:${COLORS[i % 4]}">${bal.sizes[i]} MB</span>
      </div>`;
  }).join('');
  return `
    <div class="bal-dist-bar-section">
      <div class="bal-dist-bar-label">
        <span>Data size distribution</span>
        <span>Diff: ${bal.diff} MB / Threshold: ${bal.threshold} MB</span>
      </div>
      <div class="bal-dist-rows">${rows}</div>
    </div>`;
}

function bRenderLogHTML() {
  if (!bSt.log.length) return '<div class="bal-log-empty">No activity yet.</div>';
  return bSt.log.map(e => `
    <div class="bal-log-entry bal-log-${e.type}">
      <span class="bal-log-time">${e.time}</span>
      <span class="bal-log-msg">${e.msg}</span>
    </div>`).join('');
}

// Full panel render (called on tab open, reset, shard-count change)
function renderBalancer() {
  const panel = document.getElementById('balancer-panel');
  if (!panel) return;
  if (!bSt.shards.length) bInitState(bSt.shardCount, bSt.chunksPerShard);

  const bal  = bGetBalance();
  const spLabels = ['', 'Slow', 'Normal', 'Normal', 'Fast', 'Instant'];

  panel.innerHTML = `
    <!-- How it works banner -->
    <div class="bal-intro">
      <h3>How the MongoDB Balancer Works</h3>
      <p>The balancer is a background process on the
      <span class="tip" data-tooltip="<strong>Config Server Primary</strong><br>The primary node of the Config Server Replica Set (CSRS). It stores the cluster's metadata: the chunk map, shard registry, and zone assignments. The balancer runs exclusively on this node.">config server primary</span>
      that monitors <strong>total data size per shard</strong> (MongoDB 6.0+). When the difference between the
      most-loaded and least-loaded shard exceeds the
      <span class="tip" data-tooltip="<strong>Migration Threshold</strong><br>The minimum MB difference between the largest and smallest shard before the balancer triggers chunk migrations. MongoDB's default is 500 MB. Lower = more aggressive balancing; higher = fewer migrations but more skew.">migration threshold</span>,
      it moves chunks to even out the distribution. It picks the
      <span class="tip" data-tooltip="<strong>Best-fit Chunk Selection</strong><br>The balancer picks the chunk whose sizeMB is closest to diff÷2, so after the migration the gap between shards is minimised. This avoids overshoot (where the destination shard then becomes the largest).">best-fit non-jumbo chunk</span>
      and streams it to the least-loaded shard using the
      <span class="tip" data-tooltip="<strong>Range Migration Protocol</strong><br>MongoDB's internal 3-phase chunk move: (1) clone all documents in the chunk's key range to the destination shard, (2) apply oplog deltas to stay in sync, (3) commit atomically by updating the config server's chunk map. Reads/writes continue throughout.">range migration protocol</span>.</p>
      <div class="bal-fact-grid">
        <div class="bal-fact"><span class="bal-fact-label">Balanced on:</span><span class="tip" data-tooltip="<strong>Data Size Balancing (MongoDB 6.0+)</strong><br>Replaces the old chunk-count heuristic. The balancer compares total MB on each shard rather than number of chunks, giving accurate results even when chunks vary widely in size.">Total data size (MB) per shard</span></div>
        <div class="bal-fact"><span class="bal-fact-label">Default threshold:</span><span class="tip" data-tooltip="<strong>384 MB Default (3 × chunkSize)</strong><br>MongoDB's data-size balancing threshold is 3 × chunkSize. With the default chunkSize of 128 MB that is 384 MB. Migrations only fire when max_shard − min_shard ≥ 384 MB.">384 MB (3 × 128 MB)</span></div>
        <div class="bal-fact"><span class="bal-fact-label">Chunk selection:</span><span class="tip" data-tooltip="<strong>Best-fit Selection</strong><br>Picks the chunk closest in size to diff÷2. Avoids the overshoot that a naïve largest-first strategy can cause when chunk sizes are heterogeneous.">Best-fit (minimises overshoot)</span></div>
        <div class="bal-fact"><span class="bal-fact-label">Concurrent migrations:</span><span class="tip" data-tooltip="<strong>Concurrency Limit</strong><br>MongoDB allows at most 1 active migration per shard at a time. With N shards you can have at most ⌊N/2⌋ parallel migrations in a round.">1 per shard pair</span></div>
      </div>
    </div>

    <!-- Demo layout -->
    <div class="bal-demo-layout">
      <!-- Left: controls -->
      <div class="bal-controls">
        <div class="bal-ctrl-section">
          <div class="bal-ctrl-title">Cluster Setup</div>
          <div class="bal-ctrl-row">
            <span class="bal-ctrl-label tip" data-tooltip="<strong>Shard Count</strong><br>Each shard is an independent replica set. Resets the demo cluster with an equal number of chunks distributed across all shards.">Shards</span>
            <div class="btn-group" id="bal-shard-btns">
              ${[2,3,4].map(n => `<button class="count-btn ${bSt.shardCount===n?'active':''}" data-bsn="${n}">${n}</button>`).join('')}
            </div>
          </div>
          <div class="bal-ctrl-row">
            <span class="bal-ctrl-label tip" data-tooltip="<strong>Migration Threshold</strong><br>MongoDB computes this as <strong>3 × chunkSize</strong>. Default chunkSize = 128 MB → threshold = 384 MB. Balancer only migrates when max_shard − min_shard exceeds this value.">Threshold</span>
            <div class="btn-group" id="bal-threshold-btns">
              ${[128,384,768].map(n => {
                const mult = n === 128 ? '1×' : n === 384 ? '3×' : '6×';
                const desc = n === 128 ? 'Aggressive — fires after just 1 chunk difference'
                           : n === 384 ? 'MongoDB default — 3 × 128 MB chunkSize'
                           : 'Relaxed — 6 × chunkSize, tolerates large skew';
                return `<button class="count-btn ${bSt.thresholdMB===n?'active':''}" data-btmb="${n}" data-tooltip="<strong>${mult} chunkSize threshold (${n} MB)</strong><br>${desc}">${mult}</button>`;
              }).join('')}
            </div>
          </div>
          <div class="bal-ctrl-row">
            <span class="bal-ctrl-label tip" data-tooltip="<strong>Initial Chunks per Shard</strong><br>Number of chunks each shard starts with when the cluster is reset. Each chunk has a random size between 20–100 MB.">Chunks/shard</span>
            <div class="btn-group" id="bal-cps-btns">
              ${[4,8,16].map(n => `<button class="count-btn ${bSt.chunksPerShard===n?'active':''}" data-bcps="${n}">${n}</button>`).join('')}
            </div>
          </div>
        </div>

        <div class="bal-ctrl-section">
          <div class="bal-ctrl-title">Balancer Actions</div>
          <div class="bal-threshold-info">
            <div class="bal-threshold-row"><span class="tip" data-tooltip="<strong>Threshold</strong><br>Balancer fires only when the data-size gap between the most-loaded and least-loaded shard exceeds this value. Currently set to ${bal.threshold} MB.">Threshold</span><span class="bal-threshold-val">${bal.threshold} MB</span></div>
            <div class="bal-threshold-row"><span class="tip" data-tooltip="<strong>Current Diff</strong><br>Live data-size difference (MB) between the most-loaded and least-loaded shard. When this exceeds the threshold the balancer starts migrating chunks.">Current diff</span><span class="bal-threshold-val ${bal.diff>bal.threshold?'over-threshold':''}">${bal.diff} MB</span></div>
            <div class="bal-threshold-row"><span class="tip" data-tooltip="<strong>Migrations</strong><br>Total chunk migrations completed in this session. Each migration moves one chunk from the most-loaded shard to the least-loaded shard.">Migrations</span><span class="bal-threshold-val" id="bal-migration-count">${bSt.migrations}</span></div>
          </div>
          <button class="bal-btn bal-btn-imbalance" id="bal-imbalance" data-tooltip="<strong>Trigger Imbalance</strong><br>Adds a batch of chunks to shard 0, pushing its data size well above the threshold. The balancer will then kick in and start migrating chunks.">⚡ Trigger Imbalance</button>
          <button class="bal-btn bal-btn-addshard" id="bal-add-shard" ${bSt.shardCount>=5||bSt.running?'disabled':''} data-tooltip="<strong>Add Shard Scenario</strong><br>Adds a new empty shard to the cluster. The balancer will detect the severe imbalance (new shard has 0 MB vs existing shards) and begin draining chunks from the most-loaded shards into the new one — exactly what happens in a real MongoDB scale-out.">＋ Add Shard</button>
          <button class="bal-btn ${bSt.running?'bal-btn-stop':'bal-btn-run'}" id="bal-run-toggle" data-tooltip="<strong>Start / Stop Balancer</strong><br>Runs the balancer loop automatically, migrating one chunk at a time with a short pause between rounds until the cluster is balanced.">
            ${bSt.running ? '⏹ Stop Balancer' : '▶ Start Balancer'}
          </button>
          <button class="bal-btn bal-btn-step" id="bal-step" ${bSt.running||bSt.animating?'disabled':''} data-tooltip="<strong>Step Once</strong><br>Executes a single balancer round: evaluates the current imbalance, selects the best-fit chunk, and migrates it. Useful for stepping through the algorithm manually.">⏭ Step Once</button>
          <button class="bal-btn bal-btn-jumbo" id="bal-jumbo-btn" data-tooltip="<strong>Create Jumbo Scenario</strong><br>Injects jumbo chunks on shard 0. Run the balancer to see it stall when only jumbo chunks remain.">⚠ Create Jumbo Scenario</button>
          <button class="bal-btn bal-btn-refine" id="bal-refine-key" data-tooltip="<strong>Simulate refineShardKey</strong><br>Simulates sh.refineCollectionShardKey() — splits jumbo chunks into normal movable ones so the balancer can resume redistribution.">🛠 Simulate refineShardKey</button>
          <button class="bal-btn bal-btn-jumbo" id="bal-toggle-jumbo" data-tooltip="<strong>Show / Hide Jumbo Explainer</strong><br>Toggles the jumbo-chunk concept and walkthrough section below.">${bSt.showJumbo ? '🙈 Hide Jumbo Explainer' : '📖 Show Jumbo Explainer'}</button>
          <button class="bal-btn bal-btn-reset" id="bal-reset" data-tooltip="<strong>Reset Cluster</strong><br>Destroys the current state and rebuilds the cluster with the selected shard count and chunks-per-shard, all chunks with random sizes between 20–100 MB.">↺ Reset Cluster</button>
        </div>

        <div class="bal-ctrl-section">
          <div class="bal-ctrl-title">Animation Speed</div>
          <div class="bal-speed-row">
            <input type="range" id="bal-speed" min="1" max="5" value="${bSt.speed}" class="bal-speed-slider">
            <span id="bal-speed-label">${spLabels[bSt.speed]}</span>
          </div>
        </div>
      </div>

      <!-- Right: visualization -->
      <div class="bal-viz">
        <div class="bal-viz-header">
          <div class="bal-status-badge ${bal.isBalanced?'status-balanced':bSt.running?'status-balancing':'status-imbalanced'}" id="bal-status-badge">
            ${bal.isBalanced ? '✓ BALANCED' : bSt.running ? '⟳ BALANCING…' : `⚠ IMBALANCED — diff: ${bal.diff} MB, threshold: ${bal.threshold} MB`}
          </div>
        </div>
        <div class="bal-shards" id="bal-shards">
          ${bSt.shards.map(s => bRenderShardHTML(s, bal)).join('')}
        </div>
        <div id="bal-dist-bar">${bRenderDistBarHTML(bal)}</div>
      </div>
    </div>

    <!-- Migration log -->
    <div class="bal-log-section">
      <div class="bal-log-header">
        <span>Migration Log</span>
        <button class="btn-ghost btn-xs" id="bal-clear-log">Clear</button>
      </div>
      <div class="bal-log-entries" id="bal-log-entries">${bRenderLogHTML()}</div>
    </div>

    <!-- Jumbo chunks section -->
    <div class="jumbo-section ${bSt.showJumbo ? '' : 'is-collapsed'}">
      <div class="jumbo-section-header">
        <h3>Jumbo Chunks</h3>
      </div>
      <div class="jumbo-content">
        <div class="jumbo-explanation">
          <p>A <span class="tip" data-tooltip="<strong>Jumbo Chunk</strong><br>A chunk that has grown beyond <code>chunkSize</code> (default 128 MB) and cannot be split because every document inside shares the same shard key value. MongoDB sets the <code>jumbo</code> flag in the config server and the balancer permanently skips it."><strong>jumbo chunk</strong></span>
          has grown beyond <code>chunkSize</code> (default 128 MB) and
          <em>cannot be split</em> — because every document inside shares the same
          <span class="tip" data-tooltip="<strong>Shard Key Value</strong><br>The specific value of the field used to partition data. If many documents share an identical shard key value (e.g. status='pending'), they all land in the same chunk, which grows without bound.">shard key value</span>.
          MongoDB marks it with the <code>jumbo</code> flag and the balancer skips it, so it
          accumulates indefinitely on one shard.</p>
          <div class="jumbo-facts">
            <div class="jumbo-fact jumbo-fact-red">
              <strong>Problem:</strong> The balancer cannot migrate a jumbo chunk.
              It marks it <code>jumbo: true</code> in the config server and routes all
              subsequent writes for that key to the same shard, compounding the imbalance.
            </div>
            <div class="jumbo-fact jumbo-fact-yellow">
              <strong>Cause:</strong> High write rate to a
              <span class="tip" data-tooltip="<strong>Low-Cardinality Shard Key</strong><br>Cardinality = number of distinct values. A key like <code>status</code> (3 values: pending/active/done) means at most 3 chunks can ever exist, regardless of data size. A monotonically growing key like <code>_id</code> or <code>timestamp</code> has high cardinality but creates range hotspots.">low-cardinality shard key value</span> —
              e.g. all documents with <code>status: "pending"</code> land in one chunk until
              it exceeds <code>maxChunkSize</code> and can never be halved.
            </div>
            <div class="jumbo-fact jumbo-fact-green">
              <strong>Fix:</strong> Choose a higher-cardinality shard key (or compound key).
              In MongoDB, you can refine an existing key pattern with
              <span class="tip" data-tooltip="<strong>sh.refineCollectionShardKey()</strong><br>Refines a collection's shard key by adding one or more suffix fields to increase cardinality and improve distribution/splitability without fully resharding."><code>sh.refineCollectionShardKey()</code></span>
              to make jumbo ranges splittable.
              After reducing the data, use
              <span class="tip" data-tooltip="<strong>sh.clearJumboFlag()</strong><br>Removes the <code>jumbo</code> flag from a chunk in the config server. After clearing, the balancer can attempt to split the chunk (if it can be split) and then migrate it. Requires the chunk's data to have been reduced first, otherwise it immediately becomes jumbo again."><code>sh.clearJumboFlag()</code></span> or
              <code>db.adminCommand({ clearJumboFlag: "db.coll", ... })</code> to unlock the chunk.
            </div>
          </div>
        </div>
        <div class="jumbo-demo-area" id="jumbo-demo-area" style="display:none"></div>
      </div>
    </div>
  `;

  initBalancerListeners();
  updateHeaderStats();
}

// Partial refresh — only updates mutable parts without re-creating the whole panel
function bRefreshViz() {
  updateHeaderStats();
  const bal = bGetBalance();

  // Status badge
  const badge = document.getElementById('bal-status-badge');
  if (badge) {
    badge.className = `bal-status-badge ${bal.isBalanced ? 'status-balanced' : bSt.running ? 'status-balancing' : 'status-imbalanced'}`;
    badge.textContent = bal.isBalanced ? '✓ BALANCED'
      : bSt.running ? '⟳ BALANCING…'
      : `⚠ IMBALANCED — diff: ${bal.diff} MB, threshold: ${bal.threshold} MB`;
  }

  // Per-shard counts + fills
  bSt.shards.forEach(s => {
    const chunks  = bChunksOf(s.id);
    const countEl = document.querySelector(`#bal-shard-${s.id} .bal-shard-count`);
    const footEl  = document.querySelector(`#bal-shard-${s.id} .bal-shard-footer`);
    const isMax   = s.id === bal.maxIdx && bal.diff > bal.threshold;
    const isMin   = s.id === bal.minIdx && bal.diff > bal.threshold;
    if (countEl) {
      const totalMB = chunks.reduce((a, c) => a + c.sizeMB, 0).toFixed(0);
      countEl.textContent = `${totalMB} MB`;
      countEl.className   = `bal-shard-count ${isMax ? 'most' : isMin ? 'least' : 'mid'}`;
    }
    if (footEl) {
      const totalMB = chunks.reduce((a, c) => a + c.sizeMB, 0).toFixed(0);
      footEl.textContent = `${chunks.length} chunks · ${totalMB} MB total`;
    }
  });

  // Distribution bar
  const distEl = document.getElementById('bal-dist-bar');
  if (distEl) distEl.innerHTML = bRenderDistBarHTML(bal);

  // Migration counter
  const mc = document.getElementById('bal-migration-count');
  if (mc) mc.textContent = bSt.migrations;

  // Threshold diff
  const diffEl = document.querySelector('.bal-threshold-row .bal-threshold-val.over-threshold, .bal-threshold-row .bal-threshold-val:nth-child(2)');
  // Re-render threshold info section instead
  const thInfo = document.querySelector('.bal-threshold-info');
  if (thInfo) {
    thInfo.innerHTML = `
      <div class="bal-threshold-row"><span>Threshold</span><span class="bal-threshold-val">${bal.threshold} MB</span></div>
      <div class="bal-threshold-row"><span>Current diff</span><span class="bal-threshold-val ${bal.diff>bal.threshold?'over-threshold':''}">${bal.diff} MB</span></div>
      <div class="bal-threshold-row"><span>Migrations</span><span class="bal-threshold-val" id="bal-migration-count">${bSt.migrations}</span></div>`;
  }
}

// ----- migration animation -----

async function bAnimateMigration(fromShardIdx, toShardIdx, chunk) {
  bSt.animating = true;

  // 1. Mark chunk as leaving on source
  const chunkEl = document.getElementById(`bch-${chunk.id}`);
  if (chunkEl) chunkEl.classList.add('migrating-out');

  await sleep(bSpeedMs() * 0.55);

  // 2. Move chunk in state
  chunk.shardIdx    = toShardIdx;
  chunk.justArrived = true;
  chunk.sizeMB      = +chunk.sizeMB.toFixed(1); // keep same size

  // 3. Re-render both shard chunk lists
  const fromEl = document.getElementById(`bal-chunks-${fromShardIdx}`);
  const toEl   = document.getElementById(`bal-chunks-${toShardIdx}`);
  if (fromEl) fromEl.innerHTML = bChunksOf(fromShardIdx).map(bRenderChunkHTML).join('');
  if (toEl)   toEl.innerHTML   = bChunksOf(toShardIdx).map(bRenderChunkHTML).join('');

  // 4. Remove justArrived flag after animation completes
  setTimeout(() => {
    chunk.justArrived = false;
    const arrived = document.getElementById(`bch-${chunk.id}`);
    if (arrived) arrived.classList.remove('just-arrived');
  }, bSpeedMs() * 0.6);

  await sleep(bSpeedMs() * 0.5);

  bRefreshViz();
  bSt.animating = false;
}

// ----- one balancer step -----

async function bDoStep() {
  if (bSt.animating) return false;

  const bal = bGetBalance();
  if (bal.isBalanced) {
    bLog('done', `✓ Cluster balanced. Total migrations: ${bSt.migrations}.`);
    bSt.running = false;
    bRefreshViz();
    bUpdateRunBtn();
    return false;
  }

  const srcChunks = bChunksOf(bal.maxIdx);

  // Best-fit: pick the chunk whose sizeMB is closest to diff/2 (minimises overshoot)
  const candidates = srcChunks.filter(c => !c.isJumbo).sort((a, b) => {
    const target = bal.diff / 2;
    return Math.abs(a.sizeMB - target) - Math.abs(b.sizeMB - target);
  });
  const jumbos     = srcChunks.filter(c => c.isJumbo);

  if (!candidates.length && jumbos.length) {
    // All moveable chunks already migrated; only jumbos remain
    bLog('jumbo', `⚠ ${jumbos.length} jumbo chunk(s) on ${bSt.shards[bal.maxIdx].name} cannot be migrated. Balancer stalled.`);
    bLog('jumbo', `Run <code>sh.clearJumboFlag()</code> then resplit to unblock.`);
    bSt.running = false;
    bRefreshViz();
    bUpdateRunBtn();
    return false;
  }

  if (jumbos.length) {
    bLog('jumbo', `⚠ Skipping ${jumbos.length} jumbo chunk(s) on ${bSt.shards[bal.maxIdx].name}.`);
  }

  const chunk = candidates[0];
  const target = +(bal.diff / 2).toFixed(1);
  const afterDiff = +(bal.diff - chunk.sizeMB * 2).toFixed(1);
  const afterLabel = Math.abs(afterDiff) <= bal.threshold ? 'balanced ✓' : `diff ~${Math.abs(afterDiff)} MB`;
  bLog('migrate',
    `Moving <strong>${chunk.id}</strong> (${chunk.sizeMB} MB) · <strong>${bSt.shards[bal.maxIdx].name}</strong> → <strong>${bSt.shards[bal.minIdx].name}</strong>`);
  bLog('info',
    `↳ Why this chunk? Imbalance = ${bal.diff} MB → ideal move ≈ ${target} MB. `
    + `${chunk.id} at ${chunk.sizeMB} MB is the closest fit among ${candidates.length} candidates. `
    + `After migration: ${afterLabel}.`);

  await bAnimateMigration(bal.maxIdx, bal.minIdx, chunk);
  bSt.migrations++;
  return true;
}

function bUpdateRunBtn() {
  const btn = document.getElementById('bal-run-toggle');
  if (!btn) return;
  btn.className = `bal-btn ${bSt.running ? 'bal-btn-stop' : 'bal-btn-run'}`;
  btn.innerHTML = bSt.running ? '⏹ Stop Balancer' : '▶ Start Balancer';
  const stepBtn = document.getElementById('bal-step');
  if (stepBtn) stepBtn.disabled = bSt.running || bSt.animating;
  const addBtn = document.getElementById('bal-add-shard');
  if (addBtn) addBtn.disabled = bSt.running || bSt.shardCount >= 5;
}

async function bRunLoop() {
  while (bSt.running) {
    const moved = await bDoStep();
    if (!moved) break;
    await sleep(bSpeedMs() * 0.3); // gap between steps
  }
  bSt.running = false;
  bUpdateRunBtn();
}

// ----- trigger imbalance -----

function bTriggerImbalance() {
  if (bSt.running || bSt.animating) return;
  // Add chunks totalling ~700–1200 MB extra to shard 0, ensuring diff > threshold
  const target = bSt.thresholdMB + 200 + Math.random() * 500;
  let added = 0;
  let addedMB = 0;
  while (addedMB < target) {
    const sz = 50 + Math.random() * 100;
    bSt.chunks.push(bNewChunk(0, sz));
    addedMB += sz;
    added++;
  }
  const bal = bGetBalance();
  bLog('warn', `Added ${added} chunks (~${addedMB.toFixed(0)} MB) to ${bSt.shards[0].name}. New diff: ${bal.diff} MB (threshold: ${bal.threshold} MB) — balancer should trigger.`);

  // Re-render shard 0's chunk list
  const chunksEl = document.getElementById('bal-chunks-0');
  if (chunksEl) chunksEl.innerHTML = bChunksOf(0).map(bRenderChunkHTML).join('');
  bRefreshViz();
}

// ----- jumbo chunk scenario -----

function bShowJumboModal() {
  const existing = document.getElementById('jumbo-modal-overlay');
  if (existing) existing.remove();

  const overlay = document.createElement('div');
  overlay.id = 'jumbo-modal-overlay';
  overlay.className = 'jumbo-modal-overlay';
  overlay.innerHTML = `
    <div class="jumbo-modal">
      <div class="jumbo-modal-header">
        <span class="jumbo-modal-icon">⚠</span>
        <h3>What is a Jumbo Chunk?</h3>
        <button class="jumbo-modal-close" id="jumbo-modal-close">×</button>
      </div>
      <div class="jumbo-modal-body">
        <div class="jumbo-modal-section">
          <h4>How jumbo chunks are created</h4>
          <p>A chunk becomes <strong>jumbo</strong> when it grows beyond the configured <code>chunkSize</code> (default 128 MB) and MongoDB cannot split it. This happens when every document in the chunk shares the <strong>same shard key value</strong> — for example, all documents with <code>status: &quot;pending&quot;</code> must live in the same chunk, and that chunk cannot be halved because there is no boundary to split on.</p>
          <div class="jumbo-modal-code">
            // Example: low-cardinality key causes jumbo chunks<br>
            sh.shardCollection(&quot;mydb.orders&quot;, { status: 1 })<br><br>
            // All &quot;pending&quot; orders accumulate in one chunk<br>
            // until it exceeds chunkSize and gains the jumbo flag
          </div>
        </div>
        <div class="jumbo-modal-section">
          <h4>Risks of jumbo chunks</h4>
          <ul class="jumbo-modal-list">
            <li><span class="risk-badge risk-high">High</span><strong>Balancer cannot migrate them.</strong> The balancer permanently skips chunks flagged <code>jumbo: true</code>, so data skew on the hot shard is never resolved automatically.</li>
            <li><span class="risk-badge risk-high">High</span><strong>Persistent hot shard.</strong> Writes continue routing to the same shard indefinitely, compounding CPU, memory, and I/O pressure on that node.</li>
            <li><span class="risk-badge risk-med">Medium</span><strong>Replication lag.</strong> A single overloaded primary shard can slow oplog application on secondaries, increasing replication lag cluster-wide.</li>
            <li><span class="risk-badge risk-med">Medium</span><strong>Difficult to resolve at scale.</strong> Clearing the jumbo flag (<code>sh.clearJumboFlag()</code>) only works once the underlying data distribution or shard key is improved.</li>
            <li><span class="risk-badge risk-low">Low</span><strong>Metadata bloat.</strong> Jumbo chunks are tracked indefinitely in the config server, adding noise to chunk-map queries and balancer decisions.</li>
          </ul>
        </div>
        <div class="jumbo-modal-section">
          <h4>Remediation path</h4>
          <p>Use <code>sh.refineCollectionShardKey()</code> to add a higher-cardinality suffix field to the shard key, enabling MongoDB to split the previously unsplittable range. Then clear the flag and let the balancer resume.</p>
        </div>
      </div>
      <div class="jumbo-modal-footer">
        <button class="btn-primary" id="jumbo-modal-confirm">Create Jumbo Scenario →</button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);

  document.getElementById('jumbo-modal-close').addEventListener('click', () => overlay.remove());
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
  document.getElementById('jumbo-modal-confirm').addEventListener('click', () => {
    overlay.remove();
    bCreateJumboScenario();
  });
}

function bCreateJumboScenario() {
  if (bSt.running) return;
  bSt.showJumbo = true;

  // Add 3 jumbo chunks to shard 0 (marking them isJumbo=true)
  const jumboSizes = [156, 182, 143];
  jumboSizes.forEach(sz => bSt.chunks.push(bNewChunk(0, sz, true)));

  // Add some normal extra chunks so the imbalance triggers
  for (let i = 0; i < 6; i++) bSt.chunks.push(bNewChunk(0, 40 + Math.random() * 50));

  const bal = bGetBalance();
  bLog('jumbo', `Created 3 jumbo chunks on ${bSt.shards[0].name}. Diff: ${bal.diff} MB — balancer will skip jumbo chunks.`);

  // Refresh shard 0
  const chunksEl = document.getElementById('bal-chunks-0');
  if (chunksEl) chunksEl.innerHTML = bChunksOf(0).map(bRenderChunkHTML).join('');
  bRefreshViz();

  // Show the jumbo demo area with explanation steps
  const demoArea = document.getElementById('jumbo-demo-area');
  if (!demoArea) return;
  demoArea.style.display = 'block';
  demoArea.innerHTML = `
    <div class="jumbo-demo-title">Live Scenario — Jumbo Chunk Walkthrough</div>
    <div class="jumbo-scenario-steps">
      <div class="jumbo-step step-done">
        <div class="jumbo-step-num">1</div>
        <div class="jumbo-step-text">
          <strong>Imbalance created.</strong> ${bSt.shards[0].name} has ${bChunksOf(0).length} chunks including
          <strong>3 jumbo chunks</strong> (striped red ⚠). Diff = ${bal.diff} MB &gt; threshold ${bal.threshold} MB.
        </div>
      </div>
      <div class="jumbo-step step-active">
        <div class="jumbo-step-num">2</div>
        <div class="jumbo-step-text">
          <strong>Click "Start Balancer"</strong> — the balancer evaluates ${bSt.shards[0].name} as the source.
          It will migrate normal chunks first, skipping any chunk with <code>isJumbo: true</code>.
        </div>
      </div>
      <div class="jumbo-step step-pending">
        <div class="jumbo-step-num">3</div>
        <div class="jumbo-step-text">
          <strong>Balancer stalls</strong> once only jumbo chunks remain on ${bSt.shards[0].name}.
          You will see: <em>"⚠ jumbo chunk(s) cannot be migrated"</em> in the migration log.
        </div>
      </div>
      <div class="jumbo-step step-pending">
        <div class="jumbo-step-num">4</div>
        <div class="jumbo-step-text">
          <strong>Resolution in production:</strong>
          refine shard key cardinality using
          <code>sh.refineCollectionShardKey("db.collection", { oldKey: 1, suffix: 1 })</code>,
          then clear jumbo with
          <code>sh.clearJumboFlag("db.collection", &lt;chunkQuery&gt;)</code> so split + migration can resume.
        </div>
      </div>
    </div>
    <div class="jumbo-error-msg" id="jumbo-error-msg">
      MongoServerError: Cannot migrate chunk: chunk is marked 'jumbo'. Use clearJumboFlag to clear the flag.
    </div>`;
}

function bSimulateRefineShardKey() {
  if (bSt.running || bSt.animating) return;

  const jumbos = bSt.chunks.filter(c => c.isJumbo);
  if (!jumbos.length) {
    bLog('info', 'No jumbo chunks found. Create a jumbo scenario first.');
    return;
  }

  let splitChunks = 0;
  let unlockedMB = 0;

  jumbos.forEach(j => {
    // Remove jumbo chunk
    bSt.chunks = bSt.chunks.filter(c => c.id !== j.id);

    // Simulate improved cardinality after refineShardKey by splitting range into smaller movable chunks
    const parts = j.sizeMB > 170 ? 3 : 2;
    let remaining = j.sizeMB;

    for (let i = 0; i < parts; i++) {
      let piece;
      if (i === parts - 1) {
        piece = remaining;
      } else {
        const avg = remaining / (parts - i);
        piece = +(Math.max(28, Math.min(96, avg + (Math.random() * 10 - 5))).toFixed(1));
        remaining -= piece;
      }
      bSt.chunks.push(bNewChunk(j.shardIdx, piece, false));
      splitChunks++;
    }

    unlockedMB += j.sizeMB;
  });

  bLog('done', `Simulated <code>sh.refineCollectionShardKey()</code>: ${jumbos.length} jumbo chunk(s) were split into ${splitChunks} normal chunks.`);
  bLog('info', `~${unlockedMB.toFixed(0)} MB is now movable. Start balancer to redistribute from hot shard(s).`);

  const demoArea = document.getElementById('jumbo-demo-area');
  if (demoArea && demoArea.style.display !== 'none') {
    const steps = demoArea.querySelectorAll('.jumbo-step');
    if (steps[3]) steps[3].className = 'jumbo-step step-done';
    const msg = document.createElement('div');
    msg.className = 'jumbo-fix-msg';
    msg.textContent = 'Refine shard key simulation complete. Jumbo flags cleared by resplitting. Run the balancer again to finish redistribution.';
    demoArea.appendChild(msg);
  }

  bRefreshViz();
  renderBalancer();
}

// ----- add shard scenario -----

function bAddShardScenario() {
  if (bSt.running || bSt.animating || bSt.shardCount >= 5) return;

  // Add the new shard to state
  const newId   = bSt.shards.length;
  const newName = `rs${newId + 1}`;
  bSt.shards.push({ id: newId, name: newName });
  bSt.shardCount = bSt.shards.length;

  // Re-render the full panel so the new shard column appears
  renderBalancer();

  const bal = bGetBalance();
  bLog('info', `⊕ Shard <strong>${newName}</strong> joined the cluster (0 MB). Cluster now has ${bSt.shardCount} shards.`);
  bLog('warn', `Imbalance detected — new shard is empty. Diff: ${bal.diff} MB vs threshold ${bal.threshold} MB.`);
  bLog('info', `↳ The balancer will now drain chunks from the most-loaded shards into <strong>${newName}</strong> until data is evenly distributed.`);
}

// Show the jumbo error message when balancer stalls on jumbo chunks
function bShowJumboError() {
  const el = document.getElementById('jumbo-error-msg');
  if (el) {
    el.style.display = 'block';
    // Update step 3 to active
    const steps = document.querySelectorAll('.jumbo-step');
    if (steps[2]) { steps[2].className = 'jumbo-step step-error'; }
    if (steps[3]) { steps[3].className = 'jumbo-step step-active'; }
  }
}

// ----- event wiring -----

function initBalancerListeners() {
  // Shard count buttons
  document.getElementById('bal-shard-btns')?.addEventListener('click', e => {
    const btn = e.target.closest('[data-bsn]');
    if (!btn || bSt.running) return;
    bSt.shardCount = parseInt(btn.dataset.bsn);
    bInitState(bSt.shardCount, bSt.chunksPerShard);
    renderBalancer();
    bLog('info', `Cluster reset to ${bSt.shardCount} shards × ${bSt.chunksPerShard} chunks.`);
  });

  // Threshold MB buttons
  document.getElementById('bal-threshold-btns')?.addEventListener('click', e => {
    const btn = e.target.closest('[data-btmb]');
    if (!btn || bSt.running) return;
    bSt.thresholdMB = parseInt(btn.dataset.btmb);
    document.querySelectorAll('#bal-threshold-btns .count-btn').forEach(b => b.classList.toggle('active', b === btn));
    bRefreshViz();
    const mult = bSt.thresholdMB / bSt.chunkSizeMB;
    bLog('info', `Migration threshold changed to ${bSt.thresholdMB} MB (${mult}× chunkSize).`);
  });

  // Chunks/shard buttons
  document.getElementById('bal-cps-btns')?.addEventListener('click', e => {
    const btn = e.target.closest('[data-bcps]');
    if (!btn || bSt.running) return;
    bSt.chunksPerShard = parseInt(btn.dataset.bcps);
    bInitState(bSt.shardCount, bSt.chunksPerShard);
    renderBalancer();
    bLog('info', `Cluster reset to ${bSt.shardCount} shards × ${bSt.chunksPerShard} chunks.`);
  });

  // Trigger imbalance
  document.getElementById('bal-imbalance')?.addEventListener('click', bTriggerImbalance);

  // Add shard scenario
  document.getElementById('bal-add-shard')?.addEventListener('click', bAddShardScenario);

  // Run / stop toggle
  document.getElementById('bal-run-toggle')?.addEventListener('click', () => {
    if (bSt.running) {
      bSt.running = false;
      bLog('warn', 'Balancer stopped manually.');
      bUpdateRunBtn();
    } else {
      const bal = bGetBalance();
      if (bal.isBalanced) { bLog('info', 'Cluster is already balanced.'); return; }
      bSt.running = true;
      bLog('info', `Balancer started. Diff: ${bal.diff} MB, threshold: ${bal.threshold} MB. Will run until balanced.`);
      bUpdateRunBtn();
      bRunLoop().then(() => {
        const b2 = bGetBalance();
        if (b2.isBalanced) {
          bLog('done', `✓ Cluster balanced after ${bSt.migrations} total migrations.`);
        } else if (bSt.chunks.some(c => c.isJumbo && c.shardIdx === b2.maxIdx)) {
          bShowJumboError();
        }
      });
    }
  });

  // Step once
  document.getElementById('bal-step')?.addEventListener('click', async () => {
    const bal = bGetBalance();
    if (bal.isBalanced) { bLog('info', 'Already balanced — nothing to migrate.'); return; }
    document.getElementById('bal-step').disabled = true;
    const moved = await bDoStep();
    if (!moved && bSt.chunks.some(c => c.isJumbo)) bShowJumboError();
    document.getElementById('bal-step').disabled = false;
  });

  // Reset
  document.getElementById('bal-reset')?.addEventListener('click', () => {
    bSt.running = false;
    bInitState(bSt.shardCount, bSt.chunksPerShard);
    renderBalancer();
    bLog('info', `Cluster reset: ${bSt.shardCount} shards × ${bSt.chunksPerShard} chunks.`);
  });

  // Speed slider
  document.getElementById('bal-speed')?.addEventListener('input', e => {
    bSt.speed = parseInt(e.target.value);
    const labels = ['', 'Slow', 'Normal', 'Normal', 'Fast', 'Instant'];
    const lbl = document.getElementById('bal-speed-label');
    if (lbl) lbl.textContent = labels[bSt.speed];
  });

  // Clear log
  document.getElementById('bal-clear-log')?.addEventListener('click', () => {
    bSt.log = [];
    const el = document.getElementById('bal-log-entries');
    if (el) el.innerHTML = '<div class="bal-log-empty">Log cleared.</div>';
  });

  // Jumbo scenario — show modal first
  document.getElementById('bal-jumbo-btn')?.addEventListener('click', bShowJumboModal);
  document.getElementById('bal-refine-key')?.addEventListener('click', bSimulateRefineShardKey);

  // Toggle jumbo explainer section visibility
  document.getElementById('bal-toggle-jumbo')?.addEventListener('click', () => {
    bSt.showJumbo = !bSt.showJumbo;
    renderBalancer();
    bLog('info', bSt.showJumbo ? 'Jumbo section shown.' : 'Jumbo section hidden.');
  });
}

// ===================================================================
// ===== AUTOMERGER MODULE ============================================
// ===================================================================

const aSt = {
  initialized: false,
  running: false,
  balanceRunning: false,
  animating: false,
  speed: 3,
  shardCount: 3,
  targetChunkMB: 128,
  checkIntervalSec: 30,
  balanceThresholdMB: 256,
  merges: 0,
  rounds: 0,
  balanceMoves: 0,
  chunks: [],
  log: [],
};

let aChunkSeq = 0;

function aNewChunk({ shardIdx, zone, min, max, sizeMB }) {
  aChunkSeq++;
  return {
    id: `am_${String(aChunkSeq).padStart(3, '0')}`,
    shardIdx,
    zone,
    min,
    max,
    sizeMB: +sizeMB.toFixed(1),
    justMerged: false,
  };
}

function aInitState() {
  aChunkSeq = 0;
  aSt.running = false;
  aSt.balanceRunning = false;
  aSt.animating = false;
  aSt.merges = 0;
  aSt.rounds = 0;
  aSt.balanceMoves = 0;
  aSt.log = [];

  // Seed with a mixture of mergeable and non-mergeable chunks.
  // Chunk key ranges are contiguous in steps of 10.
  aSt.chunks = [
    aNewChunk({ shardIdx: 0, zone: 'americas', min: 0,   max: 9,   sizeMB: 18 }),
    aNewChunk({ shardIdx: 0, zone: 'americas', min: 10,  max: 19,  sizeMB: 22 }),
    aNewChunk({ shardIdx: 0, zone: 'americas', min: 20,  max: 29,  sizeMB: 64 }),
    aNewChunk({ shardIdx: 0, zone: 'americas', min: 30,  max: 39,  sizeMB: 20 }),

    aNewChunk({ shardIdx: 1, zone: 'europe',   min: 40,  max: 49,  sizeMB: 14 }),
    aNewChunk({ shardIdx: 1, zone: 'europe',   min: 50,  max: 59,  sizeMB: 16 }),
    aNewChunk({ shardIdx: 1, zone: 'europe',   min: 60,  max: 69,  sizeMB: 19 }),
    aNewChunk({ shardIdx: 1, zone: 'europe',   min: 70,  max: 79,  sizeMB: 92 }),

    aNewChunk({ shardIdx: 2, zone: 'asia',     min: 80,  max: 89,  sizeMB: 23 }),
    aNewChunk({ shardIdx: 2, zone: 'europe',   min: 90,  max: 99,  sizeMB: 21 }), // same shard, different zone
    aNewChunk({ shardIdx: 2, zone: 'asia',     min: 100, max: 109, sizeMB: 17 }),
    aNewChunk({ shardIdx: 2, zone: 'asia',     min: 110, max: 119, sizeMB: 20 }),
  ];

  aSt.initialized = true;
}

function aSpeedMs() {
  return Math.max(120, 950 - aSt.speed * 160);
}

function aLog(type, msg) {
  const now = new Date().toLocaleTimeString('en-US', { hour12: false });
  aSt.log.unshift({ type, msg, time: now });
  if (aSt.log.length > 80) aSt.log.pop();
  const el = document.getElementById('am-log-entries');
  if (el) el.innerHTML = aRenderLogHTML();
}

function aShardName(i) {
  return `rs${i + 1}`;
}

function aZoneClass(zone) {
  if (zone === 'americas') return 'am-zone-americas';
  if (zone === 'europe') return 'am-zone-europe';
  return 'am-zone-asia';
}

function aCanMerge(left, right) {
  if (!left || !right) return { ok: false, reason: 'missing chunk' };
  if (left.shardIdx !== right.shardIdx) return { ok: false, reason: 'different shard' };
  if (left.zone !== right.zone) return { ok: false, reason: 'different zone' };
  if (left.max + 1 !== right.min) return { ok: false, reason: 'non-contiguous range' };
  if (left.sizeMB + right.sizeMB > aSt.targetChunkMB) return { ok: false, reason: 'combined size > target chunk size' };
  return { ok: true };
}

function aFindMergeCandidates() {
  const out = [];
  for (let s = 0; s < aSt.shardCount; s++) {
    const arr = aSt.chunks
      .filter(c => c.shardIdx === s)
      .sort((a, b) => a.min - b.min);

    for (let i = 0; i < arr.length - 1; i++) {
      const left = arr[i];
      const right = arr[i + 1];
      const check = aCanMerge(left, right);
      out.push({
        left,
        right,
        ok: check.ok,
        reason: check.reason || '',
        combinedMB: +(left.sizeMB + right.sizeMB).toFixed(1),
        shardIdx: s,
      });
    }
  }
  return out;
}

function aPickNextMerge(candidates) {
  const ok = candidates.filter(c => c.ok);
  if (!ok.length) return null;
  // Prefer smallest combined size first so we demonstrate gradual metadata consolidation.
  ok.sort((a, b) => a.combinedMB - b.combinedMB || a.left.min - b.left.min);
  return ok[0];
}

function aRenderChunkHTML(c) {
  return `
    <div class="am-chunk ${aZoneClass(c.zone)} ${c.justMerged ? 'just-merged' : ''}" id="am-chunk-${c.id}">
      <div class="am-chunk-top">
        <span class="am-chunk-id">${c.id}</span>
        <span class="am-chunk-size">${c.sizeMB} MB</span>
      </div>
      <div class="am-chunk-meta">
        <span class="am-chip">${c.min} → ${c.max}</span>
        <span class="am-chip am-chip-zone">${c.zone}</span>
      </div>
    </div>`;
}

function aRenderShardHTML(shardIdx) {
  const chunks = aSt.chunks
    .filter(c => c.shardIdx === shardIdx)
    .sort((a, b) => a.min - b.min);
  const totalMB = chunks.reduce((s, c) => s + c.sizeMB, 0).toFixed(1);
  return `
    <div class="am-shard">
      <div class="am-shard-head">
        <span class="am-shard-name">${aShardName(shardIdx)}</span>
        <span class="am-shard-total">${chunks.length} chunks · ${totalMB} MB</span>
      </div>
      <div class="am-shard-chunks">
        ${chunks.map(aRenderChunkHTML).join('')}
      </div>
    </div>`;
}

function aRenderLogHTML() {
  if (!aSt.log.length) return '<div class="am-log-empty">No activity yet.</div>';
  return aSt.log.map(e => `
    <div class="am-log-entry am-log-${e.type}">
      <span class="am-log-time">${e.time}</span>
      <span class="am-log-msg">${e.msg}</span>
    </div>
  `).join('');
}

function aRenderCandidates(candidates) {
  if (!candidates.length) return '<div class="am-empty">No adjacent pairs available.</div>';

  const rows = candidates.slice(0, 12).map(c => {
    const cls = c.ok ? 'ok' : 'blocked';
    const reason = c.ok ? 'mergeable' : c.reason;
    return `
      <div class="am-candidate ${cls}">
        <span class="am-c-left">${aShardName(c.shardIdx)} · ${c.left.min}-${c.left.max} + ${c.right.min}-${c.right.max}</span>
        <span class="am-c-mid">${c.combinedMB} MB</span>
        <span class="am-c-right">${reason}</span>
      </div>`;
  }).join('');

  return `<div class="am-candidate-list">${rows}</div>`;
}

function aStats(candidates) {
  const mergeable = candidates.filter(c => c.ok).length;
  const blocked = candidates.length - mergeable;
  return { mergeable, blocked, pairs: candidates.length };
}

function aGetBalance() {
  const sizes = Array.from({ length: aSt.shardCount }, (_, s) =>
    +aSt.chunks.filter(c => c.shardIdx === s).reduce((sum, c) => sum + c.sizeMB, 0).toFixed(1)
  );
  const maxSize = Math.max(...sizes);
  const minSize = Math.min(...sizes);
  const maxIdx = sizes.indexOf(maxSize);
  const minIdx = sizes.indexOf(minSize);
  const diff = +(maxSize - minSize).toFixed(1);
  return {
    sizes,
    maxSize,
    minSize,
    maxIdx,
    minIdx,
    diff,
    threshold: aSt.balanceThresholdMB,
    isBalanced: diff <= aSt.balanceThresholdMB,
  };
}

function aPickNextBalanceMove(bal) {
  const sourceChunks = aSt.chunks
    .filter(c => c.shardIdx === bal.maxIdx)
    .sort((a, b) => Math.abs(a.sizeMB - bal.diff / 2) - Math.abs(b.sizeMB - bal.diff / 2));
  return sourceChunks[0] || null;
}

function renderAutoMerger() {
  const panel = document.getElementById('automerger-panel');
  if (!panel) return;
  if (!aSt.initialized) aInitState();

  const candidates = aFindMergeCandidates();
  const stats = aStats(candidates);
  const next = aPickNextMerge(candidates);
  const bal = aGetBalance();
  const speedLabels = ['', 'Slow', 'Normal', 'Normal', 'Fast', 'Instant'];

  panel.innerHTML = `
    <div class="am-intro">
      <h3>How AutoMerger Works for Chunks</h3>
      <p>
        AutoMerger periodically scans chunk metadata and merges <strong>adjacent chunks on the same shard</strong>
        when preconditions are satisfied (same shard, contiguous ranges, compatible zone/tag context, and
        resulting chunk size under target). This reduces chunk-map cardinality and metadata overhead.
      </p>
      <div class="am-note">
        Important: this demo focuses on merge mechanics. Exact cadence and controls can vary by MongoDB version and deployment.
      </div>
      <div class="am-policy-tip">
        <div class="am-policy-head">Policy levers you can tune</div>
        <ul>
          <li><strong>Enable/disable AutoMerger:</strong> control AutoMerger with <code>configureCollectionBalancing</code> using the <code>enableAutoMerger</code> setting (collection-level policy).</li>
          <li><strong>Check interval:</strong> tune how frequently the AutoMerger/balancer pass evaluates merge candidates using the <code>autoMergerIntervalSecs</code> balancing setting.</li>
          <li><strong>Balancer window and state:</strong> AutoMerger work is tied to balancer activity, so <code>sh.startBalancer()</code>, <code>sh.stopBalancer()</code>, and balancing windows affect when merges can run.</li>
          <li><strong>Merge eligibility rules:</strong> only adjacent chunk ranges that are merge-compatible (same shard and compatible zone/tag constraints) are candidates.</li>
          <li><strong>Chunk size policy:</strong> merges are constrained by configured chunk-size limits (for example via <code>configureCollectionBalancing</code> options), so oversized merged ranges are skipped.</li>
        </ul>
      </div>
      <div class="am-facts">
        <div class="am-fact"><span>Moves data between shards:</span><strong>No</strong></div>
        <div class="am-fact"><span>Merges occur on:</span><strong>Same shard only</strong></div>
        <div class="am-fact"><span>Current merge target:</span><strong>${aSt.targetChunkMB} MB</strong></div>
        <div class="am-fact"><span>Check interval:</span><strong>${aSt.checkIntervalSec}s</strong></div>
      </div>
    </div>

    <div class="am-layout">
      <div class="am-controls">
        <div class="am-ctrl-card">
          <div class="am-ctrl-title">AutoMerger Controls</div>

          <div class="am-ctrl-row">
            <span>Target chunk size</span>
            <div class="btn-group" id="am-target-btns">
              ${[64, 128, 192].map(v => `<button class="count-btn ${aSt.targetChunkMB===v?'active':''}" data-am-target="${v}">${v}</button>`).join('')}
            </div>
          </div>

          <div class="am-ctrl-row">
            <span class="tip" data-tooltip="<strong>AutoMerger Check Interval</strong><br>MongoDB exposes this as <code>autoMergerIntervalSecs</code> in collection balancing settings.<br><br><strong>Example</strong><br><code>db.adminCommand({ configureCollectionBalancing: \"db.orders\", autoMergerIntervalSecs: 30 })</code><br><br>This UI control simulates that setting.">Check interval (sec)</span>
            <div class="btn-group" id="am-interval-btns">
              ${[15, 30, 60].map(v => `<button class="count-btn ${aSt.checkIntervalSec===v?'active':''}" data-am-interval="${v}">${v}</button>`).join('')}
            </div>
          </div>

          <button class="bal-btn bal-btn-imbalance" id="am-fragment">✂ Inject Fragmentation</button>
          <button class="bal-btn ${aSt.running?'bal-btn-stop':'bal-btn-run'}" id="am-run-toggle">${aSt.running ? '⏹ Stop AutoMerger' : '▶ Start AutoMerger'}</button>
          <button class="bal-btn bal-btn-step" id="am-step" ${aSt.running||aSt.animating?'disabled':''}>⏭ Step Once</button>
          <button class="bal-btn bal-btn-reset" id="am-reset">↺ Reset Scenario</button>
        </div>

        <div class="am-ctrl-card">
          <div class="am-ctrl-title">Balancer Controls (Same Tab)</div>

          <div class="am-ctrl-row">
            <span>Rebalance threshold (MB)</span>
            <div class="btn-group" id="am-balance-threshold-btns">
              ${[128, 256, 384].map(v => `<button class="count-btn ${aSt.balanceThresholdMB===v?'active':''}" data-am-bt="${v}">${v}</button>`).join('')}
            </div>
          </div>

          <button class="bal-btn bal-btn-imbalance" id="am-hotshard">⚡ Create Hot Shard</button>
          <button class="bal-btn ${aSt.balanceRunning?'bal-btn-stop':'bal-btn-run'}" id="am-balance-toggle" ${aSt.running?'disabled':''}>${aSt.balanceRunning ? '⏹ Stop Balancer' : '▶ Start Balancer'}</button>
          <button class="bal-btn bal-btn-step" id="am-balance-step" ${aSt.running||aSt.balanceRunning||aSt.animating?'disabled':''}>⏭ Balance Step</button>

          <div class="am-balance-state ${bal.isBalanced ? 'ok' : 'warn'}">
            <div><span>Current diff</span><strong>${bal.diff} MB</strong></div>
            <div><span>Threshold</span><strong>${bal.threshold} MB</strong></div>
            <div><span>Moves</span><strong>${aSt.balanceMoves}</strong></div>
          </div>
        </div>

        <div class="am-ctrl-card">
          <div class="am-ctrl-title">Round Status</div>
          <div class="am-status-grid">
            <div><span>Pairs checked</span><strong>${stats.pairs}</strong></div>
            <div><span>Mergeable</span><strong class="ok">${stats.mergeable}</strong></div>
            <div><span>Blocked</span><strong class="blocked">${stats.blocked}</strong></div>
            <div><span>Merges done</span><strong>${aSt.merges}</strong></div>
          </div>
          <div class="am-next-merge ${next ? '' : 'none'}">
            ${next
              ? `Next: ${aShardName(next.shardIdx)} · ${next.left.min}-${next.left.max} + ${next.right.min}-${next.right.max} → ${(next.left.min)}-${(next.right.max)} (${next.combinedMB} MB)`
              : 'No eligible pair remains under current rules.'}
          </div>

          <div class="am-speed-row">
            <input type="range" id="am-speed" min="1" max="5" value="${aSt.speed}" class="bal-speed-slider">
            <span id="am-speed-label">${speedLabels[aSt.speed]}</span>
          </div>
        </div>
      </div>

      <div class="am-viz">
        <div class="am-shard-grid">
          ${Array.from({ length: aSt.shardCount }, (_, i) => aRenderShardHTML(i)).join('')}
        </div>
      </div>
    </div>

    <div class="am-candidates-wrap">
      <div class="am-candidates-head">Adjacency checks this round</div>
      ${aRenderCandidates(candidates)}
    </div>

    <div class="am-jumbo-section">
      <div class="am-jumbo-head">Jumbo Chunks — Concept and Operational Impact</div>
      <div class="am-jumbo-grid">
        <div class="am-jumbo-card">
          <h4>What is a jumbo chunk?</h4>
          <p>
            A chunk is marked <code>jumbo: true</code> when it grows beyond configured chunk-size limits
            and cannot be split effectively (commonly because many documents share the same shard-key value).
          </p>
        </div>
        <div class="am-jumbo-card">
          <h4>Why it matters</h4>
          <p>
            The balancer typically skips jumbo chunks, so data skew can persist on a hot shard even when balancing is enabled.
            AutoMerger does not resolve this condition because the root issue is splitability and key distribution.
          </p>
        </div>
        <div class="am-jumbo-card">
          <h4>Typical causes</h4>
          <p>
            Low-cardinality keys, monotonic write patterns, or extreme frequency skew on specific shard-key values
            can concentrate growth in one chunk and create recurring jumbo conditions.
          </p>
        </div>
        <div class="am-jumbo-card">
          <h4>Common remediation path</h4>
          <p>
            Improve shard-key design and data distribution first, then clear the jumbo flag when appropriate
            (for example with <code>clearJumboFlag</code> / <code>sh.clearJumboFlag()</code>) so normal split/migration
            behavior can resume.
          </p>
        </div>
      </div>
    </div>

    <div class="am-log-section">
      <div class="am-log-head">
        <span>AutoMerger Log</span>
        <button class="btn-ghost btn-xs" id="am-clear-log">Clear</button>
      </div>
      <div class="am-log-entries" id="am-log-entries">${aRenderLogHTML()}</div>
    </div>
  `;

  initAutoMergerListeners();
  updateHeaderStats();
}

async function aAnimateMerge(pair) {
  aSt.animating = true;
  const lEl = document.getElementById(`am-chunk-${pair.left.id}`);
  const rEl = document.getElementById(`am-chunk-${pair.right.id}`);
  if (lEl) lEl.classList.add('am-merging-out');
  if (rEl) rEl.classList.add('am-merging-out');
  await sleep(aSpeedMs() * 0.6);

  aSt.chunks = aSt.chunks.filter(c => c.id !== pair.left.id && c.id !== pair.right.id);
  const merged = aNewChunk({
    shardIdx: pair.left.shardIdx,
    zone: pair.left.zone,
    min: pair.left.min,
    max: pair.right.max,
    sizeMB: pair.combinedMB,
  });
  merged.justMerged = true;
  aSt.chunks.push(merged);

  renderAutoMerger();
  setTimeout(() => {
    const c = aSt.chunks.find(x => x.id === merged.id);
    if (c) c.justMerged = false;
    const el = document.getElementById(`am-chunk-${merged.id}`);
    if (el) el.classList.remove('just-merged');
  }, aSpeedMs() * 0.8);

  aSt.animating = false;
}

async function aAnimateBalanceMove(chunk, toShardIdx) {
  aSt.animating = true;
  const el = document.getElementById(`am-chunk-${chunk.id}`);
  if (el) el.classList.add('am-merging-out');
  await sleep(aSpeedMs() * 0.55);
  chunk.shardIdx = toShardIdx;
  chunk.justMerged = true;
  renderAutoMerger();
  setTimeout(() => {
    const c = aSt.chunks.find(x => x.id === chunk.id);
    if (c) c.justMerged = false;
  }, aSpeedMs() * 0.8);
  aSt.animating = false;
}

async function aDoStep() {
  if (aSt.animating) return false;
  aSt.rounds++;

  const candidates = aFindMergeCandidates();
  const next = aPickNextMerge(candidates);
  if (!next) {
    aLog('done', 'No eligible adjacent pair remains. AutoMerger is idle under current thresholds and zone/shard constraints.');
    aSt.running = false;
    renderAutoMerger();
    return false;
  }

  aLog('merge', `Round ${aSt.rounds}: merging ${next.left.id} (${next.left.min}-${next.left.max}, ${next.left.sizeMB} MB) + ${next.right.id} (${next.right.min}-${next.right.max}, ${next.right.sizeMB} MB) on <strong>${aShardName(next.shardIdx)}</strong>.`);
  aLog('info', `↳ Merge is metadata-local to ${aShardName(next.shardIdx)} (no cross-shard data move). New chunk size: ${next.combinedMB} MB.`);

  await aAnimateMerge(next);
  aSt.merges++;
  return true;
}

async function aRunLoop() {
  while (aSt.running) {
    const moved = await aDoStep();
    if (!moved) break;
    await sleep(aSpeedMs() * 0.35);
  }
  aSt.running = false;
  renderAutoMerger();
}

async function aDoBalanceStep() {
  if (aSt.animating) return false;
  const bal = aGetBalance();
  if (bal.isBalanced) {
    aLog('done', `Balancer idle: shard diff ${bal.diff} MB is within threshold (${bal.threshold} MB).`);
    aSt.balanceRunning = false;
    renderAutoMerger();
    return false;
  }

  const chunk = aPickNextBalanceMove(bal);
  if (!chunk) {
    aLog('warn', 'No movable chunk found on the hottest shard.');
    aSt.balanceRunning = false;
    renderAutoMerger();
    return false;
  }

  aLog('merge', `Balancer move: ${chunk.id} (${chunk.sizeMB} MB) ${aShardName(bal.maxIdx)} → ${aShardName(bal.minIdx)}.`);
  aLog('info', '↳ This demonstrates that after chunk merges reduce metadata, the balancer can still redistribute chunks to resolve hot-shard skew.');
  await aAnimateBalanceMove(chunk, bal.minIdx);
  aSt.balanceMoves++;
  return true;
}

async function aBalanceRunLoop() {
  while (aSt.balanceRunning) {
    const moved = await aDoBalanceStep();
    if (!moved) break;
    await sleep(aSpeedMs() * 0.35);
  }
  aSt.balanceRunning = false;
  renderAutoMerger();
}

function aInjectFragmentation() {
  if (aSt.running || aSt.balanceRunning || aSt.animating) return;

  const shard = 1;
  const zone = 'europe';
  const start = 120;
  const sizes = [11, 13, 12, 10, 15, 12];

  sizes.forEach((sz, i) => {
    const min = start + i * 10;
    const max = min + 9;
    aSt.chunks.push(aNewChunk({ shardIdx: shard, zone, min, max, sizeMB: sz }));
  });

  aLog('warn', `Injected ${sizes.length} tiny contiguous chunks on ${aShardName(shard)} (${zone}) to simulate post-split fragmentation.`);
  renderAutoMerger();
}

function aCreateHotShard() {
  if (aSt.running || aSt.balanceRunning || aSt.animating) return;
  const shard = 0;
  const zone = 'americas';
  const maxRange = Math.max(...aSt.chunks.map(c => c.max));
  let nextMin = maxRange + 1;
  let addedMB = 0;
  let added = 0;

  while (addedMB < 420) {
    const size = 38 + Math.random() * 44;
    const c = aNewChunk({ shardIdx: shard, zone, min: nextMin, max: nextMin + 9, sizeMB: size });
    aSt.chunks.push(c);
    nextMin += 10;
    addedMB += size;
    added++;
  }

  const bal = aGetBalance();
  aLog('warn', `Created hot shard by adding ${added} chunks (~${addedMB.toFixed(0)} MB) to ${aShardName(shard)}. Diff is now ${bal.diff} MB.`);
  renderAutoMerger();
}

function initAutoMergerListeners() {
  document.getElementById('am-target-btns')?.addEventListener('click', e => {
    const btn = e.target.closest('[data-am-target]');
    if (!btn || aSt.running || aSt.balanceRunning) return;
    aSt.targetChunkMB = parseInt(btn.dataset.amTarget, 10);
    aLog('info', `Target chunk size updated to ${aSt.targetChunkMB} MB.`);
    renderAutoMerger();
  });

  document.getElementById('am-interval-btns')?.addEventListener('click', e => {
    const btn = e.target.closest('[data-am-interval]');
    if (!btn || aSt.running || aSt.balanceRunning) return;
    aSt.checkIntervalSec = parseInt(btn.dataset.amInterval, 10);
    aLog('info', `AutoMerger check interval set to ${aSt.checkIntervalSec}s (simulation cadence).`);
    renderAutoMerger();
  });

  document.getElementById('am-balance-threshold-btns')?.addEventListener('click', e => {
    const btn = e.target.closest('[data-am-bt]');
    if (!btn || aSt.running || aSt.balanceRunning) return;
    aSt.balanceThresholdMB = parseInt(btn.dataset.amBt, 10);
    aLog('info', `Balancer threshold changed to ${aSt.balanceThresholdMB} MB.`);
    renderAutoMerger();
  });

  document.getElementById('am-fragment')?.addEventListener('click', aInjectFragmentation);
  document.getElementById('am-hotshard')?.addEventListener('click', aCreateHotShard);

  document.getElementById('am-run-toggle')?.addEventListener('click', () => {
    if (aSt.balanceRunning) return;
    if (aSt.running) {
      aSt.running = false;
      aLog('warn', 'AutoMerger stopped manually.');
      renderAutoMerger();
      return;
    }

    const next = aPickNextMerge(aFindMergeCandidates());
    if (!next) {
      aLog('info', 'Nothing to merge at current settings. Try a larger target chunk size or inject fragmentation.');
      return;
    }

    aSt.running = true;
    aLog('info', `AutoMerger started (interval ${aSt.checkIntervalSec}s simulated cadence).`);
    renderAutoMerger();
    aRunLoop();
  });

  document.getElementById('am-balance-toggle')?.addEventListener('click', () => {
    if (aSt.running) return;
    if (aSt.balanceRunning) {
      aSt.balanceRunning = false;
      aLog('warn', 'Balancer stopped manually.');
      renderAutoMerger();
      return;
    }

    const bal = aGetBalance();
    if (bal.isBalanced) {
      aLog('info', `Cluster already balanced (diff ${bal.diff} MB ≤ threshold ${bal.threshold} MB).`);
      return;
    }

    aSt.balanceRunning = true;
    aLog('info', `Balancer started at threshold ${aSt.balanceThresholdMB} MB.`);
    renderAutoMerger();
    aBalanceRunLoop();
  });

  document.getElementById('am-step')?.addEventListener('click', async () => {
    if (aSt.running || aSt.balanceRunning) return;
    const btn = document.getElementById('am-step');
    if (btn) btn.disabled = true;
    await aDoStep();
    if (btn) btn.disabled = false;
  });

  document.getElementById('am-balance-step')?.addEventListener('click', async () => {
    if (aSt.running || aSt.balanceRunning) return;
    const btn = document.getElementById('am-balance-step');
    if (btn) btn.disabled = true;
    await aDoBalanceStep();
    if (btn) btn.disabled = false;
  });

  document.getElementById('am-reset')?.addEventListener('click', () => {
    aInitState();
    renderAutoMerger();
    aLog('info', 'AutoMerger scenario reset to defaults.');
  });

  document.getElementById('am-speed')?.addEventListener('input', e => {
    aSt.speed = parseInt(e.target.value, 10);
    const labels = ['', 'Slow', 'Normal', 'Normal', 'Fast', 'Instant'];
    const lbl = document.getElementById('am-speed-label');
    if (lbl) lbl.textContent = labels[aSt.speed] || 'Normal';
  });

  document.getElementById('am-clear-log')?.addEventListener('click', () => {
    aSt.log = [];
    const el = document.getElementById('am-log-entries');
    if (el) el.innerHTML = '<div class="am-log-empty">Log cleared.</div>';
  });
}

// ===== INIT =====

// ===================================================================
// ===== TOOLTIP ENGINE ===============================================
// ===================================================================

function initTooltip() {
  const tip = document.createElement('div');
  tip.id = 'app-tooltip';
  document.body.appendChild(tip);

  function show(e) {
    const target = e.target.closest('[data-tooltip]');
    if (!target) return;
    tip.innerHTML = target.dataset.tooltip;
    tip.classList.add('visible');
    position(e);
  }
  function hide(e) {
    const target = e.target.closest('[data-tooltip]');
    if (target) tip.classList.remove('visible');
  }
  function position(e) {
    const gap = 14;
    let x = e.clientX + gap;
    let y = e.clientY + gap;
    // Measure after content is set
    const w = tip.offsetWidth, h = tip.offsetHeight;
    if (x + w > window.innerWidth  - 8) x = e.clientX - w - gap;
    if (y + h > window.innerHeight - 8) y = e.clientY - h - gap;
    tip.style.left = x + 'px';
    tip.style.top  = y + 'px';
  }

  document.addEventListener('mouseover',  show);
  document.addEventListener('mousemove',  e => { if (tip.classList.contains('visible')) position(e); });
  document.addEventListener('mouseout',   hide);
}

document.addEventListener('DOMContentLoaded', () => {
  initTooltip();
  initEventListeners();
  renderAll();
  addLog('info', 'Demo loaded. Press <strong>Space</strong> or click "Insert Document" to begin routing simulation.');
});
