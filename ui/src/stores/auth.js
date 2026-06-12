import { defineStore } from 'pinia'
import { ref } from 'vue'
import { api } from '../api/http.js'

export const useAuthStore = defineStore('auth', () => {
  // user shape: { username, role, authKind, expiresAt }
  const user = ref(null)

  async function fetchMe() {
    try {
      user.value = await api.me()
      return user.value
    } catch (e) {
      user.value = null
      throw e
    }
  }

  async function login(username, password) {
    const r = await api.login(username, password)
    user.value = { ...r, authKind: 'session' }
    return user.value
  }

  async function logout() {
    try { await api.logout() } catch (_) {}
    user.value = null
  }

  function clearUser() {
    user.value = null
  }

  function isWriter() {
    return user.value && user.value.role !== 'viewer'
  }

  return { user, fetchMe, login, logout, clearUser, isWriter }
})
