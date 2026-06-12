<script setup>
import { ref, onMounted, onUnmounted, computed } from 'vue'
import { ElMessage } from 'element-plus'
import { api } from '../api/http.js'

const entries = ref([])      // newest first
const loading = ref(true)
const autoRefresh = ref(true)
const filter = ref('all')    // all | login | op | failover

let timer = null
const REFRESH_MS = 3000

async function loadInitial() {
  try {
    const r = await api.audit(null, 100)
    entries.value = r.entries || []
  } catch (e) {
    ElMessage.warning('加载审计失败: ' + (e.message || e))
  } finally {
    loading.value = false
  }
}

// Pull only entries newer than the most recent one we already have.
async function pullDelta() {
  if (!autoRefresh.value) return
  const firstStreamId = entries.value.find(e => e.source === 'ui-audit')?.id
  if (!firstStreamId) {
    // No anchor yet — re-do full initial load.
    await loadInitial()
    return
  }
  try {
    const r = await api.audit(firstStreamId, 100)
    const fresh = (r.entries || []).filter(e => e.source === 'ui-audit')
    if (fresh.length) {
      // Newest first.
      entries.value = [...fresh.sort((a, b) => b.ts - a.ts), ...entries.value]
      // Cap memory at 500 entries.
      if (entries.value.length > 500) entries.value.length = 500
    }
  } catch (e) { /* silent during background polling */ }
}

onMounted(async () => {
  await loadInitial()
  timer = setInterval(pullDelta, REFRESH_MS)
})
onUnmounted(() => { if (timer) clearInterval(timer) })

const filteredEntries = computed(() => {
  if (filter.value === 'all') return entries.value
  return entries.value.filter(e => {
    const t = (e.type || '').toLowerCase()
    if (filter.value === 'login') return t.startsWith('login.')
    if (filter.value === 'op') return t.startsWith('op.')
    if (filter.value === 'failover') return t.startsWith('op.failover') ||
      t.startsWith('op.switchover') || t === 'failover.record'
    return true
  })
})

function tagType(type) {
  if (!type) return 'info'
  if (type.startsWith('login.success')) return 'success'
  if (type.startsWith('login.failure')) return 'danger'
  if (type.startsWith('login.locked'))  return 'warning'
  if (type.startsWith('op.'))           return 'primary'
  if (type === 'failover.record')       return 'info'
  return 'info'
}

function formatTs(ts) {
  if (!ts) return '—'
  const d = new Date(typeof ts === 'string' ? Number(ts) : ts)
  return d.toLocaleString()
}

function detailsText(details) {
  if (!details) return ''
  if (typeof details === 'string') return details
  return Object.entries(details)
    .filter(([_, v]) => v !== '' && v !== null && v !== undefined)
    .map(([k, v]) => `${k}=${v}`).join(', ')
}
</script>

<template>
  <div>
    <div style="display:flex; align-items: center; justify-content: space-between; margin-bottom: 12px;">
      <h2 style="margin: 0;">审计</h2>
      <div>
        <el-radio-group v-model="filter" size="small" style="margin-right: 12px;">
          <el-radio-button value="all">全部</el-radio-button>
          <el-radio-button value="login">登录</el-radio-button>
          <el-radio-button value="op">操作</el-radio-button>
          <el-radio-button value="failover">Failover</el-radio-button>
        </el-radio-group>
        <el-switch v-model="autoRefresh" active-text="自动刷新" />
      </div>
    </div>

    <el-table
      v-loading="loading"
      :data="filteredEntries"
      stripe
      size="small"
      :empty-text="loading ? '加载中...' : '暂无记录'">
      <el-table-column label="时间" width="180">
        <template #default="{ row }">{{ formatTs(row.ts) }}</template>
      </el-table-column>
      <el-table-column label="类型" width="180">
        <template #default="{ row }">
          <el-tag :type="tagType(row.type)" size="small">{{ row.type }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="详情">
        <template #default="{ row }">
          <code style="font-size: 12px; color: #606266;">{{ detailsText(row.details) }}</code>
        </template>
      </el-table-column>
      <el-table-column label="来源" width="120">
        <template #default="{ row }">{{ row.source }}</template>
      </el-table-column>
    </el-table>

    <div class="muted" style="margin-top: 8px;">
      显示 {{ filteredEntries.length }} 条 · 自动每 {{ REFRESH_MS/1000 }}s 拉取增量
    </div>
  </div>
</template>
