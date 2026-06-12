<script setup>
import { ref, onMounted, computed } from 'vue'
import { ElMessage } from 'element-plus'
import { useRouter } from 'vue-router'
import { api } from '../api/http.js'
import { useAuthStore } from '../stores/auth.js'
import ConfirmDangerDialog from '../components/ConfirmDangerDialog.vue'

const router = useRouter()
const auth = useAuthStore()
const cluster = ref({ primaryNode: null, nodes: [] })
const backup = ref({ state: 'IDLE', lastBackupTime: 'never' })
const selectedNode = ref('')

const dialog = ref({
  open: false,
  title: '',
  description: '',
  expected: '',
  action: null
})

async function refresh() {
  try {
    cluster.value = await api.clusterStatus()
    backup.value = await api.backupStatus()
    if (!selectedNode.value && cluster.value.nodes.length) {
      const firstStandby = cluster.value.nodes.find(n => n.role === 'STANDBY')
      if (firstStandby) selectedNode.value = firstStandby.id
    }
  } catch (e) { /* http wrapper handles 401 */ }
}
onMounted(refresh)

const standbyNodes = computed(() =>
  cluster.value.nodes.filter(n => n.role === 'STANDBY'))

const allNodes = computed(() => cluster.value.nodes)

const isWriter = computed(() => auth.isWriter())

function openDialog(opts) {
  dialog.value = { ...opts, open: true }
}

async function onConfirm() {
  const action = dialog.value.action
  dialog.value.open = false
  if (!action) return
  try {
    const result = await action()
    ElMessage.success(result?.status || '操作已发起')
    router.push('/audit')
  } catch (e) {
    if (e.status === 403) {
      ElMessage.error('当前用户为只读 (viewer)，无权执行该操作')
    } else {
      ElMessage.error(e.message || '操作失败')
    }
  }
}

function startFailover() {
  const primary = cluster.value.primaryNode
  openDialog({
    title: 'Failover (故障切换)',
    description: `将立即在 HAProxy 上阻塞 ${primary} 的写流量，并选出新的 primary 接管。此操作不可撤销。`,
    expected: primary || 'FAILOVER',
    action: () => api.failover(primary)
  })
}

function startSwitchover() {
  if (!selectedNode.value) { ElMessage.warning('请选择目标 standby'); return }
  openDialog({
    title: 'Switchover (有计划切换)',
    description: `当前 primary ${cluster.value.primaryNode} → 切换至 ${selectedNode.value}。会先封写、drain CDC，再切换。`,
    expected: selectedNode.value,
    action: () => api.switchover(selectedNode.value)
  })
}

function startFullsync() {
  if (!selectedNode.value) { ElMessage.warning('请选择目标节点'); return }
  openDialog({
    title: 'Full Sync (全量重建)',
    description: `将清空 ${selectedNode.value} 的本地图并从 primary 全量重新导入，期间该节点不可读。`,
    expected: selectedNode.value,
    action: () => api.fullsync(selectedNode.value)
  })
}

async function backupPrepare() {
  if (!selectedNode.value) { ElMessage.warning('请选择目标节点'); return }
  try {
    const r = await api.backupPrepare(selectedNode.value)
    ElMessage.success(`Backup prepared at ${r.prepareTime}`)
    refresh()
  } catch (e) { ElMessage.error(e.message) }
}

async function backupComplete() {
  try {
    await api.backupComplete()
    ElMessage.success('Backup completed')
    refresh()
  } catch (e) { ElMessage.error(e.message) }
}
</script>

<template>
  <div>
    <div class="page-title">
      <h2>运维操作</h2>
      <div class="meta">
        Primary: <strong>{{ cluster.primaryNode || '—' }}</strong>
      </div>
    </div>

    <el-alert
      v-if="!isWriter"
      type="info" show-icon :closable="false"
      title="当前用户为只读 (viewer) — 写操作按钮已禁用"
      style="margin-bottom: 16px;" />

    <el-card style="margin-bottom: 18px;">
      <template #header><strong>目标节点</strong></template>
      <el-select v-model="selectedNode" placeholder="选择目标节点" style="width: 320px;">
        <el-option
          v-for="n in allNodes" :key="n.id"
          :label="`${n.id}  ·  ${n.role}  ·  ${n.health}`"
          :value="n.id" />
      </el-select>
      <p class="muted" style="margin-top: 10px;">
        Failover 不需要选择目标（自动按 lag 最低优先）；其他操作必须先选目标节点。
        节点详情请查看右侧"集群拓扑"面板。
      </p>
    </el-card>

    <div class="card-grid">
      <el-card>
        <template #header><strong>Failover (故障切换)</strong></template>
        <p class="muted">由健康检查不通过时自动触发；手工执行用于演练或加速恢复。</p>
        <el-button type="danger" :disabled="!isWriter" @click="startFailover">
          强制 Failover 当前 Primary
        </el-button>
      </el-card>

      <el-card>
        <template #header><strong>Switchover (有计划切换)</strong></template>
        <p class="muted">将选中的 standby 提升为 primary，drain 期间写不可用约 1-3s。</p>
        <el-button type="warning" :disabled="!isWriter || !selectedNode" @click="startSwitchover">
          切换至 {{ selectedNode || '...' }}
        </el-button>
      </el-card>

      <el-card>
        <template #header><strong>Full Sync (全量重建)</strong></template>
        <p class="muted">清空目标 standby 的本地图，从 primary 全量重新导入。</p>
        <el-button type="primary" :disabled="!isWriter || !selectedNode" @click="startFullsync">
          对 {{ selectedNode || '...' }} 执行全量同步
        </el-button>
      </el-card>

      <el-card>
        <template #header>
          <div style="display:flex; justify-content: space-between; align-items: center;">
            <strong>Backup（节点维护态）</strong>
            <el-tag size="small" :type="backup.state === 'IDLE' ? 'info' : 'warning'">
              {{ backup.state }}
            </el-tag>
          </div>
        </template>
        <p class="muted">
          <strong>Prepare</strong> 把节点切到维护态：暂停同步 + 摘除 HAProxy 读路由 +
          暂停健康检查告警。<br>
          <strong>之后请在宿主机执行</strong> <code>docker stop</code> /
          <code>cp 数据目录</code> / <code>docker start</code>，完成后点
          <strong>Complete</strong> 恢复。<br>
          Last: {{ backup.lastBackupTime }}。超过 2h 自动恢复（防脚本异常）。
        </p>
        <el-button :disabled="!isWriter || !selectedNode" @click="backupPrepare">
          Prepare {{ selectedNode || '...' }}
        </el-button>
        <el-button type="success" :disabled="!isWriter" @click="backupComplete" style="margin-left: 8px;">
          Complete
        </el-button>
      </el-card>
    </div>

    <ConfirmDangerDialog
      v-model="dialog.open"
      :title="dialog.title"
      :description="dialog.description"
      :expected-confirmation="dialog.expected"
      @confirm="onConfirm" />
  </div>
</template>
