<script setup>
import { ref, computed, watch } from 'vue'
import { ElMessage } from 'element-plus'

const props = defineProps({
  modelValue: { type: Boolean, required: true },
  title: { type: String, required: true },
  // Must be typed exactly by the operator to enable the confirm button.
  expectedConfirmation: { type: String, default: '' },
  description: { type: String, default: '' },
  confirmLabel: { type: String, default: '执行' },
  confirmType: { type: String, default: 'danger' }
})
const emit = defineEmits(['update:modelValue', 'confirm'])

const typed = ref('')
const visible = computed({
  get: () => props.modelValue,
  set: (v) => emit('update:modelValue', v)
})

watch(() => props.modelValue, (v) => { if (!v) typed.value = '' })

const canConfirm = computed(() =>
  !props.expectedConfirmation || typed.value === props.expectedConfirmation
)

function onConfirm() {
  if (!canConfirm.value) {
    ElMessage.warning('请准确输入确认文本')
    return
  }
  emit('confirm')
}
</script>

<template>
  <el-dialog v-model="visible" :title="title" width="460px" :close-on-click-modal="false">
    <p v-if="description" style="line-height: 1.6;">{{ description }}</p>
    <el-alert
      v-if="expectedConfirmation"
      type="warning" show-icon :closable="false"
      :title="`请输入 ${expectedConfirmation} 以确认`"
      style="margin: 12px 0;" />
    <el-input
      v-if="expectedConfirmation"
      v-model="typed"
      :placeholder="expectedConfirmation"
      @keyup.enter="canConfirm && onConfirm()" />
    <template #footer>
      <el-button @click="visible = false">取消</el-button>
      <el-button :type="confirmType" :disabled="!canConfirm" @click="onConfirm">
        {{ confirmLabel }}
      </el-button>
    </template>
  </el-dialog>
</template>
