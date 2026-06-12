<script setup>
import { ref, watch, onMounted, onUnmounted } from 'vue'
import * as echarts from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { GridComponent, TooltipComponent } from 'echarts/components'
import { SVGRenderer } from 'echarts/renderers'

echarts.use([LineChart, GridComponent, TooltipComponent, SVGRenderer])

const props = defineProps({
  // points: [{ ts: <ms>, value: <number> }] OR plain numbers (last N)
  points: { type: Array, required: true },
  height: { type: Number, default: 60 },
  color:  { type: String, default: '#409EFF' }
})

const wrap = ref(null)
let chart = null
let resizeObserver = null

function buildOption(points) {
  const xs = points.map((p, i) => p.ts ? new Date(p.ts).toLocaleTimeString() : i)
  const ys = points.map(p => typeof p === 'number' ? p : p.value)
  return {
    grid:   { top: 4, right: 4, bottom: 4, left: 4 },
    xAxis:  { type: 'category', show: false, data: xs },
    yAxis:  { type: 'value', show: false, min: 'dataMin', max: 'dataMax' },
    tooltip: {
      trigger: 'axis',
      formatter: params => {
        const p = params[0]
        return `${p.name}<br/>${p.value}`
      },
      axisPointer: { type: 'none' }
    },
    series: [{
      type: 'line',
      smooth: true,
      symbol: 'none',
      lineStyle: { width: 1.5, color: props.color },
      areaStyle: {
        color: {
          type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [
            { offset: 0, color: props.color + '55' },
            { offset: 1, color: props.color + '00' }
          ]
        }
      },
      data: ys
    }]
  }
}

function render() {
  if (!wrap.value) return
  if (!chart) chart = echarts.init(wrap.value, null, { renderer: 'svg' })
  chart.setOption(buildOption(props.points || []), true)
}

onMounted(() => {
  render()
  resizeObserver = new ResizeObserver(() => chart && chart.resize())
  resizeObserver.observe(wrap.value)
})

onUnmounted(() => {
  if (resizeObserver) resizeObserver.disconnect()
  if (chart) { chart.dispose(); chart = null }
})

watch(() => props.points, () => render(), { deep: true })
</script>

<template>
  <div ref="wrap" :style="{ width: '100%', height: height + 'px' }"></div>
</template>
