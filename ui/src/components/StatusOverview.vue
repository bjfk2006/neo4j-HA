<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { api } from '../api/http.js'
import MiniSparkline from './MiniSparkline.vue'

const cluster = ref({ primaryNode: null, nodes: [] })
const summary = ref(null)
const events  = ref([])
const lagHistory = ref([])      // ring buffer: most-recent first, up to 60 samples
const HISTORY_CAP = 60          // 60 * 2s ≈ 2 min sparkline window

let pollTimer = null
const POLL_MS = 2000

async function refresh() {
  try {
    const [c, s, a] = await Promise.all([
      api.clusterStatus().catch(() => null),
      api.metricsSummary().catch(() => null),
      api.audit(null, 5).catch(() => null)
    ])
    if (c) cluster.value = c
    if (s) {
      summary.value = s
      lagHistory.value.push({ ts: s.ts, value: s.syncLagMs ?? 0 })
      if (lagHistory.value.length > HISTORY_CAP) lagHistory.value.shift()
    }
    if (a) events.value = a.entries || []
  } catch (e) { /* http wrapper handles 401 */ }
}

onMounted(() => { refresh(); pollTimer = setInterval(refresh, POLL_MS) })
onUnmounted(() => { if (pollTimer) clearInterval(pollTimer) })

const nodeDotKind = (node) => {
  if (node.health === 'HEALTHY' && node.serviceState === 'ONLINE') return 'ok'
  if (node.health === 'HEALTHY' && node.serviceState === 'SYNCING') return 'warn'
  if (node.health === 'UNHEALTHY' || node.serviceState === 'OFFLINE') return 'danger'
  return 'muted'
}

const lagDisplay = computed(() => {
  const v = summary.value?.syncLagMs
  if (v === null || v === undefined) return '—'
  if (v < 1000) return `${v} ms`
  return `${(v / 1000).toFixed(1)} s`
})

const lagPeak = computed(() => {
  if (!lagHistory.value.length) return '—'
  const m = Math.max(...lagHistory.value.map(p => p.value))
  return m < 1000 ? `${m} ms` : `${(m / 1000).toFixed(1)} s`
})

const formatTime = (ts) => {
  if (!ts) return '—'
  const d = new Date(typeof ts === 'number' ? ts : Date.parse(ts))
  return d.toLocaleTimeString()
}

const tagForEvent = (type) => {
  if (!type) return ''
  if (type.startsWith('login.success')) return '登录'
  if (type.startsWith('login.failure')) return '登录失败'
  if (type.startsWith('op.failover')) return 'Failover'
  if (type.startsWith('op.switchover')) return 'Switchover'
  if (type.startsWith('op.fullsync')) return 'FullSync'
  if (type.startsWith('op.backup')) return 'Backup'
  if (type === 'failover.record') return 'Failover-Done'
  return type
}
</script>

<template>
  <!-- Cluster topology summary -->
  <div class="aside-section">
    <div class="aside-section-title">
      集群拓扑
      <span class="badge">{{ cluster.nodes.length }} 节点</span>
    </div>
    <div v-if="cluster.nodes.length === 0" class="muted">无节点数据</div>
    <div v-else>
      <div v-for="n in cluster.nodes" :key="n.id" class="node-row">
        <span class="dot" :class="nodeDotKind(n)"></span>
        <span class="node-name">{{ n.id }}</span>
        <span v-if="n.inBackup" class="maint-badge" title="维护中（备份）">🔧</span>
        <span class="node-role-tag" :class="{ primary: n.role === 'PRIMARY' }">
          {{ n.role }}
        </span>
      </div>
    </div>
  </div>

  <!-- Sync lag sparkline -->
  <div class="aside-section">
    <div class="aside-section-title">同步延迟</div>
    <div class="spark-wrap">
      <div class="spark-summary">
        <div>
          <div class="big-num">{{ lagDisplay }}</div>
          <div style="margin-top: 3px;">当前</div>
        </div>
        <div style="text-align: right;">
          <div class="big-num">{{ lagPeak }}</div>
          <div style="margin-top: 3px;">近 2 分钟峰值</div>
        </div>
      </div>
      <MiniSparkline :points="lagHistory" :height="50" color="#409EFF" />
    </div>
  </div>

  <!-- Quick metric strip -->
  <div class="aside-section">
    <div class="aside-section-title">关键指标</div>
    <div style="display:grid; grid-template-columns: 1fr 1fr; gap: 8px;">
      <div class="spark-wrap">
        <div class="muted">Fencing Token</div>
        <div class="big-num">{{ summary?.fencingToken ?? '—' }}</div>
      </div>
      <div class="spark-wrap">
        <div class="muted">Stream 长度</div>
        <div class="big-num">{{ summary?.changesStreamLen ?? '—' }}</div>
      </div>
      <div class="spark-wrap">
        <div class="muted">Failover OK</div>
        <div class="big-num" style="color:#67C23A;">
          {{ Math.round(summary?.failoverSuccessTotal ?? 0) }}
        </div>
      </div>
      <div class="spark-wrap">
        <div class="muted">Failover Fail</div>
        <div class="big-num"
             :style="{ color: (summary?.failoverFailedTotal ?? 0) > 0 ? '#F56C6C' : '#67C23A' }">
          {{ Math.round(summary?.failoverFailedTotal ?? 0) }}
        </div>
      </div>
    </div>
  </div>

  <!-- Recent events -->
  <div class="aside-section">
    <div class="aside-section-title">
      最近事件
      <span class="badge">{{ events.length }}</span>
    </div>
    <div v-if="events.length === 0" class="muted">暂无事件</div>
    <div v-else>
      <div v-for="(e, idx) in events" :key="e.id || idx" class="event-row">
        <div class="event-ts">{{ formatTime(e.ts) }}</div>
        <div class="event-type">{{ tagForEvent(e.type) }}</div>
      </div>
    </div>
  </div>
</template>
