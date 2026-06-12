import { createApp } from 'vue'
import { createPinia } from 'pinia'
import ElementPlus from 'element-plus'
import * as ElIcons from '@element-plus/icons-vue'
import 'element-plus/dist/index.css'
import './style.css'

import App from './App.vue'
import router from './router.js'

const app = createApp(App)
app.use(createPinia())
app.use(router)
app.use(ElementPlus)
for (const [name, comp] of Object.entries(ElIcons)) {
  app.component(name, comp)
}
app.mount('#app')
