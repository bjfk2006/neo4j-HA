<script setup>
import { ref, computed, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { api } from '../api/http.js'

const cluster = ref({ primaryNode: null, nodes: [] })
const diffResult = ref(null)
const loading = ref(false)
const detail = ref(null)            // currently shown DiffEntry in dialog
const showDetail = ref(false)

const params = ref({
  scope: 'recent',
  scopeArg: '',
  limit: 1000,
  type: 'both',
  nodeId: ''        // empty = all standbys
})

onMounted(async () => {
  try { cluster.value = await api.clusterStatus() } catch (e) { /* http handles 401 */ }
})

const standbyNodes = computed(() =>
  cluster.value.nodes.filter(n => n.role === 'STANDBY'))

async function scan() {
  loading.value = true
  try {
    diffResult.value = await api.dataDiff({
      scope: params.value.scope,
      scopeArg: params.value.scope === 'label' ? params.value.scopeArg : undefined,
      limit: params.value.limit,
      type: params.value.type,
      nodeId: params.value.nodeId || undefined
    })
  } catch (e) {
    if (e.status === 429) {
      ElMessage.warning(`扫描冷却中，请等待 ${e.body?.retryAfterMs} 毫秒后重试`)
    } else if (e.status === 400 && e.code === 'scope_too_wide') {
      ElMessage.warning({
        message: `大图全表扫描会拖慢主库。请选 scope=按 Label，或把数量降到 ${e.body?.maxAllowedLimitForRecent || 200}。当前图: ${e.body?.graphNodeCount} 节点`,
        duration: 8000
      })
    } else {
      ElMessage.error('扫描失败: ' + (e.message || e))
    }
  } finally {
    loading.value = false
  }
}

const standbyKeys = computed(() => {
  if (!diffResult.value) return []
  return Object.keys(diffResult.value.diff)
})

function suggestion(counts) {
  if (!counts) return ''
  const total = (counts.missing || 0) + (counts.extra || 0) + (counts.propDiff || 0)
  if (total === 0) return '✓ 数据一致'
  if (total <= 50) return '⚠ 少量差异，建议在 v1.3 单点修复（暂未上线）；当前可考虑 Full Sync'
  if (total <= 1000) return '⚠ 差异较多，建议 Full Sync 该 standby'
  return '⛔ 差异过多，强制 Full Sync'
}

function suggestionType(counts) {
  if (!counts) return ''
  const total = (counts.missing || 0) + (counts.extra || 0) + (counts.propDiff || 0)
  if (total === 0) return 'success'
  if (total <= 50) return 'info'
  if (total <= 1000) return 'warning'
  return 'danger'
}

function openDetail(entry, kindLabel) {
  detail.value = { ...entry, _category: kindLabel }
  showDetail.value = true
}

function prettyJson(obj) {
  if (obj === null || obj === undefined) return '(none)'
  try { return JSON.stringify(obj, null, 2) } catch (e) { return String(obj) }
}
</script>

<template>
  <div>
    <!-- Scan params -->
    <el-card shadow="never" style="margin-bottom: 14px;">
      <template #header><strong>扫描参数</strong></template>
      <el-form inline label-width="80px" size="small">
        <el-form-item label="范围">
          <el-select v-model="params.scope" style="width: 130px;">
            <el-option label="最近 (按 _updated_at)" value="recent" />
            <el-option label="按 Label" value="label" />
            <el-option label="随机抽样" value="random" />
          </el-select>
        </el-form-item>
        <el-form-item label="Label" v-if="params.scope === 'label'">
          <el-input v-model="params.scopeArg" placeholder="Person" style="width: 140px;" />
        </el-form-item>
        <el-form-item label="数量">
          <el-input-number v-model="params.limit"
            :min="100" :max="10000" :step="100" style="width: 130px;" />
        </el-form-item>
        <el-form-item label="类型">
          <el-select v-model="params.type" style="width: 110px;">
            <el-option label="节点+关系" value="both" />
            <el-option label="仅节点" value="node" />
            <el-option label="仅关系" value="rel" />
          </el-select>
        </el-form-item>
        <el-form-item label="目标">
          <el-select v-model="params.nodeId" placeholder="所有 standby" clearable style="width: 160px;">
            <el-option v-for="n in standbyNodes" :key="n.id"
                       :label="n.id" :value="n.id" />
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="loading" @click="scan">
            <el-icon style="margin-right: 4px;"><Search /></el-icon>
            开始扫描
          </el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <!-- Empty state -->
    <el-empty v-if="!diffResult && !loading" description="尚未扫描，点击「开始扫描」" />

    <!-- Results -->
    <div v-if="diffResult">
      <div class="muted" style="margin-bottom: 10px;">
        primary={{ diffResult.primary }} ·
        scope={{ diffResult.scope }}{{ diffResult.scopeArg ? `(${diffResult.scopeArg})` : '' }} ·
        type={{ diffResult.type }} ·
        limit={{ diffResult.limit }} ·
        用时 {{ diffResult.scanDurationMs }} ms
      </div>

      <el-card v-for="key in standbyKeys" :key="key" shadow="never" style="margin-bottom: 14px;">
        <template #header>
          <div style="display:flex; align-items:center; justify-content:space-between;">
            <strong>{{ key }} vs {{ diffResult.primary }}</strong>
            <el-tag :type="suggestionType(diffResult.diff[key].counts)" size="small">
              {{ suggestion(diffResult.diff[key].counts) }}
            </el-tag>
          </div>
        </template>

        <div v-if="diffResult.diff[key].error" class="muted">
          ⚠ 扫描出错: {{ diffResult.diff[key].error }}
        </div>

        <template v-else>
          <div class="muted" style="margin-bottom: 10px;">
            匹配 <strong>{{ diffResult.diff[key].matched }}</strong> 条 ·
            缺失 <strong style="color:#E6A23C;">{{ diffResult.diff[key].counts.missing }}</strong> ·
            多余 <strong style="color:#F56C6C;">{{ diffResult.diff[key].counts.extra }}</strong> ·
            属性异 <strong style="color:#909399;">{{ diffResult.diff[key].counts.propDiff }}</strong>
          </div>

          <el-collapse>
            <el-collapse-item v-if="diffResult.diff[key].counts.missing > 0"
                              :name="`${key}-missing`">
              <template #title>
                <el-tag type="warning" size="small" style="margin-right: 8px;">缺失</el-tag>
                Primary 有 · Standby 没有 ({{ diffResult.diff[key].counts.missing }} 条)
              </template>
              <el-table :data="diffResult.diff[key].missing" size="small" stripe>
                <el-table-column prop="kind" label="类型" width="80" />
                <el-table-column label="Labels" width="180">
                  <template #default="{ row }">{{ row.labels.join(', ') }}</template>
                </el-table-column>
                <el-table-column prop="elementId" label="ElementId" />
                <el-table-column label="操作" width="100">
                  <template #default="{ row }">
                    <el-button text type="primary" size="small"
                               @click="openDetail(row, '缺失')">详情</el-button>
                  </template>
                </el-table-column>
              </el-table>
            </el-collapse-item>

            <el-collapse-item v-if="diffResult.diff[key].counts.extra > 0"
                              :name="`${key}-extra`">
              <template #title>
                <el-tag type="danger" size="small" style="margin-right: 8px;">多余</el-tag>
                Standby 幻影 ({{ diffResult.diff[key].counts.extra }} 条)
              </template>
              <el-table :data="diffResult.diff[key].extra" size="small" stripe>
                <el-table-column prop="kind" label="类型" width="80" />
                <el-table-column label="Labels" width="180">
                  <template #default="{ row }">{{ row.labels.join(', ') }}</template>
                </el-table-column>
                <el-table-column prop="elementId" label="ElementId" />
                <el-table-column label="操作" width="100">
                  <template #default="{ row }">
                    <el-button text type="primary" size="small"
                               @click="openDetail(row, '多余')">详情</el-button>
                  </template>
                </el-table-column>
              </el-table>
            </el-collapse-item>

            <el-collapse-item v-if="diffResult.diff[key].counts.propDiff > 0"
                              :name="`${key}-propDiff`">
              <template #title>
                <el-tag size="small" style="margin-right: 8px;">属性异</el-tag>
                两侧都有但 hash 不等 ({{ diffResult.diff[key].counts.propDiff }} 条)
              </template>
              <el-table :data="diffResult.diff[key].propDiff" size="small" stripe>
                <el-table-column prop="kind" label="类型" width="80" />
                <el-table-column label="Labels" width="180">
                  <template #default="{ row }">{{ row.labels.join(', ') }}</template>
                </el-table-column>
                <el-table-column prop="elementId" label="ElementId" />
                <el-table-column label="操作" width="100">
                  <template #default="{ row }">
                    <el-button text type="primary" size="small"
                               @click="openDetail(row, '属性异')">详情</el-button>
                  </template>
                </el-table-column>
              </el-table>
            </el-collapse-item>
          </el-collapse>
        </template>
      </el-card>
    </div>

    <!-- Detail dialog -->
    <el-dialog v-model="showDetail" title="差异详情" width="720px">
      <template v-if="detail">
        <div style="margin-bottom: 10px;">
          <el-tag size="small">{{ detail._category }}</el-tag>
          <el-tag size="small" style="margin-left: 6px;">{{ detail.kind }}</el-tag>
          <span class="muted" style="margin-left: 10px;">labels: {{ (detail.labels || []).join(', ') }}</span>
        </div>
        <div class="muted" style="margin-bottom: 4px;">
          ElementId: <code>{{ detail.elementId }}</code>
        </div>

        <el-row :gutter="12" style="margin-top: 12px;">
          <el-col :span="12">
            <div class="muted" style="margin-bottom: 4px;">Primary 属性</div>
            <pre class="json-block">{{ prettyJson(detail.primaryProps) }}</pre>
            <div v-if="detail.primaryHash" class="muted" style="margin-top: 4px; font-family: monospace; font-size: 11px;">
              hash: {{ detail.primaryHash.slice(0, 16) }}...
            </div>
          </el-col>
          <el-col :span="12">
            <div class="muted" style="margin-bottom: 4px;">Standby 属性</div>
            <pre class="json-block">{{ prettyJson(detail.standbyProps) }}</pre>
            <div v-if="detail.standbyHash" class="muted" style="margin-top: 4px; font-family: monospace; font-size: 11px;">
              hash: {{ detail.standbyHash.slice(0, 16) }}...
            </div>
          </el-col>
        </el-row>

        <div v-if="detail.delta && Object.keys(detail.delta).length" style="margin-top: 12px;">
          <div class="muted" style="margin-bottom: 4px;">字段级 delta</div>
          <pre class="json-block">{{ prettyJson(detail.delta) }}</pre>
        </div>

        <el-alert
          type="info" show-icon :closable="false"
          title="v1.2 暂不提供单点修复；如需对齐请使用「运维操作」页的 Full Sync 重建该 standby"
          style="margin-top: 14px;" />
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.json-block {
  background: #f5f7fa;
  padding: 10px 12px;
  border-radius: 4px;
  font-size: 12px;
  font-family: 'SF Mono', Menlo, Consolas, monospace;
  max-height: 260px;
  overflow: auto;
  margin: 0;
  white-space: pre-wrap;
  word-break: break-all;
}
</style>
