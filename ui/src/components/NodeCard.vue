<script setup>
import { computed } from 'vue'

const props = defineProps({
  node: { type: Object, required: true }
})

const roleType = computed(() => {
  switch (props.node.role) {
    case 'PRIMARY': return 'success'
    case 'STANDBY': return 'primary'
    case 'DOWN': return 'danger'
    default: return 'info'
  }
})

const healthType = computed(() => {
  switch (props.node.health) {
    case 'HEALTHY': return 'success'
    case 'DEGRADED': return 'warning'
    case 'UNHEALTHY': return 'danger'
    default: return 'info'
  }
})

const serviceStateType = computed(() => {
  switch (props.node.serviceState) {
    case 'ONLINE': return 'success'
    case 'SYNCING': return 'warning'
    case 'OFFLINE': return 'danger'
    default: return 'info'
  }
})

const lagText = computed(() => {
  const ms = props.node.syncLagMs
  if (ms === null || ms === undefined) return '—'
  if (ms < 1000) return `${ms} ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`
  return `${(ms / 60_000).toFixed(1)} min`
})

const lagWarn = computed(() => (props.node.syncLagMs || 0) > 2000)
</script>

<template>
  <el-card shadow="hover">
    <template #header>
      <div style="display: flex; align-items: center; justify-content: space-between;">
        <span>
          <strong>{{ node.id }}</strong>
          <el-tag v-if="node.inBackup" type="warning" size="small"
                  style="margin-left: 6px;">🔧 维护中</el-tag>
        </span>
        <el-tag :type="roleType" size="small">{{ node.role }}</el-tag>
      </div>
    </template>
    <div class="kv">
      <span>健康</span>
      <el-tag :type="healthType" size="small">{{ node.health }}</el-tag>
    </div>
    <div class="kv">
      <span>服务状态</span>
      <el-tag :type="serviceStateType" size="small">{{ node.serviceState }}</el-tag>
    </div>
    <div class="kv">
      <span>同步延迟</span>
      <span :class="{ 'lag-warn': lagWarn }">{{ lagText }}</span>
    </div>
    <div class="kv muted">
      <span>Bolt</span>
      <span style="font-family: monospace; font-size: 12px;">{{ node.boltUri }}</span>
    </div>
  </el-card>
</template>

<style scoped>
.kv {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
  font-size: 14px;
}
.lag-warn { color: #E6A23C; font-weight: 600; }
</style>
