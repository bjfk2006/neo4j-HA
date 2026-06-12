<script setup>
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuthStore } from './stores/auth.js'
import StatusOverview from './components/StatusOverview.vue'

const route = useRoute()
const router = useRouter()
const auth = useAuthStore()

const isBlank = computed(() => route.meta.layout === 'blank')

// el-menu uses :default-active for highlighting current route
const activeMenu = computed(() => route.path)

async function doLogout() {
  await auth.logout()
  router.replace('/login')
}
</script>

<template>
  <!-- Login page uses its own background, skip the shell -->
  <div v-if="isBlank"><router-view /></div>

  <!-- Main 3-column layout -->
  <div v-else class="layout-shell">

    <!-- ============ Left sidebar ============ -->
    <aside class="layout-sidebar">
      <div class="brand">
        <el-icon size="22" color="#67C23A"><Connection /></el-icon>
        <span class="brand-title">KG-Neo4j HA</span>
      </div>

      <div class="user-block" v-if="auth.user">
        <el-avatar :size="36" style="background:#409EFF;">
          {{ (auth.user.username || '?').charAt(0).toUpperCase() }}
        </el-avatar>
        <div class="user-meta">
          <div class="user-name">{{ auth.user.username }}</div>
          <el-tag size="small"
                  :type="auth.user.role === 'viewer' ? 'info' : 'success'"
                  effect="dark">
            {{ auth.user.role }}
          </el-tag>
        </div>
      </div>

      <el-menu
        :default-active="activeMenu"
        class="layout-menu"
        router
        background-color="#001529"
        text-color="#cbd5e1"
        active-text-color="#fff">
        <el-menu-item index="/dashboard">
          <el-icon><Odometer /></el-icon>
          <span>仪表盘</span>
        </el-menu-item>
        <el-menu-item index="/operations">
          <el-icon><Tools /></el-icon>
          <span>运维操作</span>
        </el-menu-item>
        <el-menu-item index="/consistency">
          <el-icon><DataLine /></el-icon>
          <span>数据一致性</span>
        </el-menu-item>
        <el-menu-item index="/audit">
          <el-icon><Document /></el-icon>
          <span>审计日志</span>
        </el-menu-item>
      </el-menu>

      <div class="sidebar-footer">
        <el-button text @click="doLogout" style="color:#cbd5e1;width:100%;">
          <el-icon style="margin-right: 6px;"><SwitchButton /></el-icon>
          登出
        </el-button>
      </div>
    </aside>

    <!-- ============ Main content ============ -->
    <main class="layout-main">
      <router-view />
    </main>

    <!-- ============ Right status panel ============ -->
    <aside class="layout-aside">
      <StatusOverview />
    </aside>
  </div>
</template>
