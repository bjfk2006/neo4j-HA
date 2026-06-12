<script setup>
import { ref, reactive } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { useAuthStore } from '../stores/auth.js'

const route = useRoute()
const router = useRouter()
const auth = useAuthStore()

const form = reactive({ username: '', password: '' })
const submitting = ref(false)
const errorMsg = ref('')

async function onSubmit() {
  if (!form.username || !form.password) {
    errorMsg.value = '请输入用户名和密码'
    return
  }
  submitting.value = true
  errorMsg.value = ''
  try {
    await auth.login(form.username, form.password)
    ElMessage.success(`登录成功，欢迎 ${auth.user.username}`)
    const redirect = route.query.redirect || '/dashboard'
    router.replace(redirect)
  } catch (e) {
    if (e.status === 423) {
      const seconds = Math.ceil((e.body?.retryAfterMs || 0) / 1000)
      errorMsg.value = `登录尝试过多，请 ${seconds} 秒后重试`
    } else if (e.status === 401) {
      errorMsg.value = '用户名或密码错误'
    } else {
      errorMsg.value = e.message || '登录失败'
    }
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <div class="login-shell">
    <div class="login-card">
      <h2>KG-Neo4j HA Agent</h2>
      <div class="subtitle">管理控制台登录</div>
      <el-form @submit.prevent="onSubmit" label-position="top">
        <el-form-item label="用户名">
          <el-input v-model="form.username" autocomplete="username" autofocus />
        </el-form-item>
        <el-form-item label="密码">
          <el-input
            v-model="form.password"
            type="password"
            show-password
            autocomplete="current-password"
            @keyup.enter="onSubmit" />
        </el-form-item>
        <el-alert v-if="errorMsg" :title="errorMsg" type="error" :closable="false" style="margin-bottom: 12px;" />
        <el-button
          type="primary"
          :loading="submitting"
          @click="onSubmit"
          style="width: 100%;">
          登录
        </el-button>
      </el-form>
      <div class="login-hint">
        用户名/密码由管理员在 <code>ha-agent.yml</code> 的 <code>admin.ui.users</code> 中配置
      </div>
    </div>
  </div>
</template>

<style scoped>
.login-shell {
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(135deg, #001529 0%, #1e3a8a 100%);
}
.login-card {
  width: 360px;
  background: #fff;
  padding: 32px 28px;
  border-radius: 8px;
  box-shadow: 0 10px 30px rgba(0,0,0,0.18);
}
.login-card h2 { margin: 0 0 4px 0; font-size: 20px; }
.subtitle { color: #909399; font-size: 13px; margin-bottom: 20px; }
.login-hint { color: #909399; font-size: 12px; margin-top: 14px; line-height: 1.6; }
</style>
