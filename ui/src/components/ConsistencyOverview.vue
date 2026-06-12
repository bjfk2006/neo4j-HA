<script setup>
import { ref, computed, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { api } from '../api/http.js'

const stats = ref(null)
const loading = ref(false)
const lastScanAt = ref(null)

async function refresh() {
  loading.value = true
  try {
    stats.value = await api.dataStats()
    lastScanAt.value = new Date()
  } catch (e) {
    ElMessage.error('扫描失败: ' + (e.message || e))
  } finally {
    loading.value = false
  }
}

onMounted(refresh)

// Aggregate unique labels across all nodes for the by-label section.
const allLabels = computed(() => {
  if (!stats.value) return []
  const set = new Set()
  stats.value.nodes.forEach(n => {
    if (n.byLabel) Object.keys(n.byLabel).forEach(l => set.add(l))
  })
  return Array.from(set).sort()
})

function labelCount(node, label) {
  return node.byLabel?.[label] ?? null
}

function labelDrift(label) {
  if (!stats.value) return 0
  const values = stats.value.nodes
    .map(n => n.byLabel?.[label])
    .filter(v => v !== undefined && v !== null)
  if (values.length === 0) return 0
  return Math.max(...values) - Math.min(...values)
}

const nodeDriftValue = computed(() => stats.value?.diff?.nodeCount?.drift)
const relDriftValue  = computed(() => stats.value?.diff?.relCount?.drift)

function fmt(v) {
  if (v === null || v === undefined) return '—'
  if (typeof v === 'number') return v.toLocaleString()
  return v
}
</script>

<template>
  <div>
    <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:14px;">
      <div>
        <div v-if="lastScanAt" class="muted">
          上次扫描: {{ lastScanAt.toLocaleString() }}
          <template v-if="stats">
            &nbsp;·&nbsp; 用时 {{ stats.scanDurationMs }} ms
          </template>
        </div>
      </div>
      <el-button type="primary" :loading="loading" @click="refresh">
        <el-icon style="margin-right: 4px;"><Refresh /></el-icon>
        重新扫描
      </el-button>
    </div>

    <el-row :gutter="12" style="margin-bottom:18px;" v-if="stats">
      <el-col :span="8">
        <el-card shadow="never">
          <div class="muted">节点数 drift</div>
          <div style="font-size: 22px; font-weight: 600;"
               :style="{ color: nodeDriftValue > 0 ? '#F56C6C' : '#67C23A' }">
            {{ fmt(nodeDriftValue) }}
          </div>
          <div class="muted" style="margin-top:4px;">
            max {{ fmt(stats.diff.nodeCount.max) }} · min {{ fmt(stats.diff.nodeCount.min) }}
          </div>
        </el-card>
      </el-col>
      <el-col :span="8">
        <el-card shadow="never">
          <div class="muted">关系数 drift</div>
          <div style="font-size: 22px; font-weight: 600;"
               :style="{ color: relDriftValue > 0 ? '#F56C6C' : '#67C23A' }">
            {{ fmt(relDriftValue) }}
          </div>
          <div class="muted" style="margin-top:4px;">
            max {{ fmt(stats.diff.relCount.max) }} · min {{ fmt(stats.diff.relCount.min) }}
          </div>
        </el-card>
      </el-col>
      <el-col :span="8">
        <el-card shadow="never">
          <div class="muted">总体状态</div>
          <div style="font-size: 22px; font-weight: 600;"
               :style="{ color: (nodeDriftValue || relDriftValue) > 0 ? '#E6A23C' : '#67C23A' }">
            {{ (nodeDriftValue || relDriftValue) > 0 ? '⚠ 有差异' : '✓ 一致' }}
          </div>
          <div class="muted" style="margin-top:4px;">
            建议：{{ (nodeDriftValue || relDriftValue) > 0 ? '切换到明细对比 tab 查看' : '无需动作' }}
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-card shadow="never" style="margin-bottom:16px;" v-if="stats">
      <template #header><strong>各节点计数</strong></template>
      <el-table :data="stats.nodes" size="small" stripe>
        <el-table-column prop="id" label="节点" width="140">
          <template #default="{ row }">
            <strong>{{ row.id }}</strong>
            <el-tag size="small" style="margin-left:6px;"
                    :type="row.role === 'PRIMARY' ? 'success' : 'primary'">
              {{ row.role }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="health" label="健康" width="100">
          <template #default="{ row }">
            <el-tag size="small" :type="row.health === 'HEALTHY' ? 'success' : 'danger'">
              {{ row.health }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="节点数" width="140">
          <template #default="{ row }">
            <span v-if="row.error" class="muted">— ({{ row.error }})</span>
            <span v-else>{{ fmt(row.nodeCount) }}</span>
          </template>
        </el-table-column>
        <el-table-column label="关系数" width="140">
          <template #default="{ row }">
            <span v-if="row.error" class="muted">—</span>
            <span v-else>{{ fmt(row.relCount) }}</span>
          </template>
        </el-table-column>
        <el-table-column label="查询耗时">
          <template #default="{ row }">
            <span class="muted">{{ row.queryDurationMs }} ms</span>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-card shadow="never" v-if="stats && allLabels.length > 0">
      <template #header>
        <strong>按 Label 分布</strong>
        <span class="muted" style="margin-left: 10px;">共 {{ allLabels.length }} 个 label</span>
      </template>
      <el-table :data="allLabels.map(l => ({ label: l, drift: labelDrift(l) }))"
                size="small" stripe>
        <el-table-column prop="label" label="Label" />
        <el-table-column v-for="node in stats.nodes" :key="node.id"
                         :label="node.id" width="140">
          <template #default="{ row }">
            {{ fmt(labelCount(node, row.label)) }}
          </template>
        </el-table-column>
        <el-table-column label="drift" width="100">
          <template #default="{ row }">
            <el-tag v-if="row.drift > 0" type="warning" size="small">
              {{ row.drift }}
            </el-tag>
            <el-tag v-else type="success" size="small">0</el-tag>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>
