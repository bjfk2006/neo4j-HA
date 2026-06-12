<script setup>
import { ref } from 'vue'
import ConsistencyOverview from '../components/ConsistencyOverview.vue'
import ConsistencyDiffTable from '../components/ConsistencyDiffTable.vue'

const activeTab = ref('overview')
</script>

<template>
  <div>
    <div class="page-title">
      <h2>数据一致性</h2>
      <div class="meta">校验主备节点的数据漂移；只读不修复</div>
    </div>

    <el-tabs v-model="activeTab" type="border-card">
      <el-tab-pane label="概览（数量统计）" name="overview">
        <ConsistencyOverview />
      </el-tab-pane>
      <el-tab-pane label="明细对比（条目级差异）" name="diff">
        <ConsistencyDiffTable />
      </el-tab-pane>
    </el-tabs>
  </div>
</template>
