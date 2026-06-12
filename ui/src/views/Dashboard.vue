<script setup>
import { ref, onMounted, onUnmounted, computed } from 'vue'
import { ElMessage } from 'element-plus'
import MetricCard from '../components/MetricCard.vue'
import { api } from '../api/http.js'

const summary = ref(null)
const cluster = ref({ primaryNode: null, nodes: [] })
const loading = ref(true)
const lastUpdate = ref(null)

let timer = null
const REFRESH_MS = 2000

async function refresh() {
  try {
    const [c, s] = await Promise.all([
      api.clusterStatus().catch(() => null),
      api.metricsSummary().catch(() => null)
    ])
    if (c) cluster.value = c
    if (s) summary.value = s
    lastUpdate.value = new Date()
  } catch (e) {
    ElMessage.warning('刷新失败: ' + (e.message || e))
  } finally {
    loading.value = false
  }
}

onMounted(() => { refresh(); timer = setInterval(refresh, REFRESH_MS) })
onUnmounted(() => { if (timer) clearInterval(timer) })

const lagTone = computed(() => {
  const lag = summary.value?.syncLagMs ?? 0
  if (lag > 5000) return 'danger'
  if (lag > 2000) return 'warn'
  return 'ok'
})

const lagDisplay = computed(() => {
  const lag = summary.value?.syncLagMs
  if (lag === null || lag === undefined) return '—'
  if (lag < 1000) return `${lag} ms`
  return `${(lag / 1000).toFixed(1)} s`
})

const failoverFailedTone = computed(() =>
  (summary.value?.failoverFailedTotal || 0) > 0 ? 'warn' : 'ok')

const standbyCount = computed(() =>
  cluster.value.nodes.filter(n => n.role === 'STANDBY').length)

const onlineCount = computed(() =>
  cluster.value.nodes.filter(n => n.serviceState === 'ONLINE').length)

function formatCount(v) {
  if (v === null || v === undefined) return '—'
  if (typeof v !== 'number') return String(v)
  if (v < 1000) return String(Math.round(v))
  if (v < 1_000_000) return (v / 1000).toFixed(1) + 'k'
  return (v / 1_000_000).toFixed(2) + 'M'
}
</script>

<template>
  <div>
    <div class="page-title">
      <h2>仪表盘</h2>
      <div class="meta">
        Primary: <strong>{{ cluster.primaryNode || '—' }}</strong>
        &nbsp;·&nbsp; ONLINE {{ onlineCount }}/{{ cluster.nodes.length }}
        &nbsp;·&nbsp; 上次刷新: {{ lastUpdate ? lastUpdate.toLocaleTimeString() : '—' }}
      </div>
    </div>

    <div class="metric-grid">
      <MetricCard label="集群规模"
        :value="`${cluster.nodes.length} 节点`"
        :hint="`${standbyCount} standby`" />
      <MetricCard label="最大同步延迟"
        :value="lagDisplay" :tone="lagTone"
        hint="primary → standby" />
      <MetricCard label="Fencing Token"
        :value="summary?.fencingToken ?? '—'"
        hint="failover 计数（Redis）" />
      <MetricCard label="活跃会话"
        :value="summary?.uiSessionActive ?? 0"
        hint="UI session 数" />
      <MetricCard label="Stream 长度"
        :value="summary?.changesStreamLen ?? '—'"
        hint="changes stream 当前条数" />
      <MetricCard label="CDC 累计发布"
        :value="formatCount(summary?.cdcEventsPublishedTotal)"
        hint="自启动以来" />
      <MetricCard label="Sync 累计应用"
        :value="formatCount(summary?.syncEventsAppliedTotal)"
        hint="自启动以来" />
      <MetricCard label="Failover 成功"
        :value="formatCount(summary?.failoverSuccessTotal)" tone="ok" />
      <MetricCard label="Failover 失败"
        :value="formatCount(summary?.failoverFailedTotal)" :tone="failoverFailedTone" />
      <MetricCard label="CDC Poll 错误"
        :value="formatCount(summary?.cdcPollErrorsTotal)"
        :tone="(summary?.cdcPollErrorsTotal || 0) > 0 ? 'warn' : 'ok'" />
    </div>
  </div>
</template>
