import { createRouter, createWebHistory } from 'vue-router'
import { useAuthStore } from './stores/auth.js'

const routes = [
  { path: '/login', name: 'login', component: () => import('./views/Login.vue'),
    meta: { layout: 'blank' } },
  { path: '/', redirect: '/dashboard' },
  { path: '/dashboard', name: 'dashboard',
    component: () => import('./views/Dashboard.vue') },
  { path: '/operations', name: 'operations',
    component: () => import('./views/Operations.vue') },
  { path: '/audit', name: 'audit',
    component: () => import('./views/Audit.vue') },
  { path: '/consistency', name: 'consistency',
    component: () => import('./views/Consistency.vue') }
]

const router = createRouter({
  history: createWebHistory('/'),
  routes
})

router.beforeEach(async (to) => {
  if (to.name === 'login') return true
  const auth = useAuthStore()
  if (!auth.user) {
    try { await auth.fetchMe() } catch (_) {}
  }
  if (!auth.user) {
    return { name: 'login', query: { redirect: to.fullPath } }
  }
  return true
})

export default router
