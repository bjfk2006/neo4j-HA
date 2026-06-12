<script setup>
defineProps({
  label: { type: String, required: true },
  value: { type: [String, Number], default: '—' },
  hint:  { type: String, default: '' },
  tone:  { type: String, default: 'default' } // default | warn | danger | ok
})
</script>

<template>
  <div class="metric-card" :class="`tone-${tone}`">
    <div class="label">{{ label }}</div>
    <div class="value">{{ value }}</div>
    <div v-if="hint" class="hint">{{ hint }}</div>
  </div>
</template>

<style scoped>
.metric-card {
  background: #fff;
  border-radius: 6px;
  padding: 14px 16px;
  border-left: 4px solid #909399;
  min-height: 76px;
  display: flex; flex-direction: column; justify-content: center;
}
.metric-card.tone-ok     { border-left-color: #67C23A; }
.metric-card.tone-warn   { border-left-color: #E6A23C; }
.metric-card.tone-danger { border-left-color: #F56C6C; }
.metric-card .label { color: #909399; font-size: 12px; margin-bottom: 6px; }
.metric-card .value { font-size: 22px; font-weight: 600; color: #303133; }
.metric-card .hint  { color: #C0C4CC; font-size: 11px; margin-top: 4px; }
</style>
