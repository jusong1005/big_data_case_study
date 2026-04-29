<template>
  <main class="screen-shell">
    <header class="screen-topbar">
      <RouterLink class="nav-link" to="/">用户画像</RouterLink>
      <div>
        <p class="eyebrow">Realtime Monitor</p>
        <h1>广电大数据实时监控大屏</h1>
      </div>
      <span class="clock">{{ currentTime }}</span>
    </header>

    <section class="screen-metrics">
      <article class="metric-card">
        <span>总订单数</span>
        <strong>{{ formatNumber(realtime.totalOrders) }}</strong>
        <small>累计订单</small>
      </article>
      <article class="metric-card">
        <span>总营业额</span>
        <strong>{{ formatMoney(realtime.totalcost) }}</strong>
        <small>单位：元</small>
      </article>
      <article class="metric-card">
        <span>有效订单</span>
        <strong>{{ formatNumber(realtime.totalValidOrders) }}</strong>
        <small>过滤无效后</small>
      </article>
      <article class="metric-card">
        <span>SVM 准确率</span>
        <strong>79.25%</strong>
        <small>AUC: 0.8223</small>
      </article>
      <article class="metric-card">
        <span>画像标签数</span>
        <strong>9</strong>
        <small>消费 / 业务 / 预测</small>
      </article>
    </section>

    <section class="screen-grid">
      <article class="panel">
        <div class="panel-header">
          <h3>标签用户分布</h3>
          <span class="data-badge">MySQL</span>
        </div>
        <EChart :option="parentOption" />
      </article>

      <article class="panel trend-panel">
        <div class="panel-header">
          <h3>订单趋势</h3>
          <span class="data-badge">Redis</span>
        </div>
        <EChart :option="trendOption" />
      </article>

      <article class="panel">
        <div class="panel-header">
          <h3>消费水平分布</h3>
          <span class="data-badge">MySQL</span>
        </div>
        <EChart :option="consumeOption" />
      </article>

      <article class="panel">
        <div class="panel-header">
          <h3>业务品牌占比</h3>
          <span class="data-badge">MySQL</span>
        </div>
        <EChart :option="brandOption" />
      </article>

      <article class="panel">
        <div class="panel-header">
          <h3>用户画像标签滚动</h3>
          <span class="data-badge">MySQL</span>
        </div>
        <div class="scroll-list">
          <div class="scroll-inner">
            <div v-for="item in rollingStats" :key="`${item.parent_label}-${item.label}`" class="scroll-row">
              <span>{{ item.parent_label }}</span>
              <strong>{{ item.label }}</strong>
              <em>{{ formatNumber(item.cnt) }}人</em>
            </div>
          </div>
        </div>
      </article>
    </section>
  </main>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue';
import { ElMessage } from 'element-plus';
import type { EChartsOption } from 'echarts';
import EChart from '@/components/EChart.vue';
import { fetchLabelStats, fetchParentStats, fetchRealtimeData } from '@/api/dashboard';
import type { LabelStat, ParentStat, RealtimeData } from '@/types/dashboard';

const realtime = ref<RealtimeData>({});
const labelStats = ref<LabelStat[]>([]);
const parentStats = ref<ParentStat[]>([]);
const currentTime = ref('');
let clockTimer: number | undefined;
let realtimeTimer: number | undefined;

function updateClock() {
  currentTime.value = new Date().toLocaleString('zh-CN', { hour12: false });
}

function formatNumber(value?: string | number) {
  const num = Number(value || 0);
  return Number.isFinite(num) ? num.toLocaleString() : '--';
}

function formatMoney(value?: string | number) {
  const num = Number(value || 0);
  return Number.isFinite(num) ? `¥${num.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ',')}` : '--';
}

async function loadRealtime() {
  try {
    realtime.value = await fetchRealtimeData();
  } catch (error) {
    ElMessage.warning('实时数据加载失败');
  }
}

async function loadStats() {
  try {
    const [parents, labels] = await Promise.all([fetchParentStats(), fetchLabelStats()]);
    parentStats.value = parents;
    labelStats.value = labels;
  } catch (error) {
    ElMessage.error('标签统计加载失败');
  }
}

const rollingStats = computed(() => labelStats.value.slice(0, 30));

function labelGroup(name: string) {
  return labelStats.value
    .filter((row) => row.parent_label === name)
    .map((row) => ({ name: row.label, value: Number(row.cnt) }));
}

const trendOption = computed<EChartsOption>(() => {
  const hourly = realtime.value.hourly || [];
  return {
    tooltip: { trigger: 'axis' },
    legend: { data: ['订单数', '营业额'], textStyle: { color: '#a8b5cc' } },
    grid: { top: 45, left: 52, right: 52, bottom: 32 },
    xAxis: { type: 'category', data: hourly.map((item) => item.hour), axisLabel: { color: '#7d8aa3' } },
    yAxis: [
      { type: 'value', axisLabel: { color: '#7d8aa3' }, splitLine: { lineStyle: { color: '#243249' } } },
      { type: 'value', axisLabel: { color: '#7d8aa3' }, splitLine: { show: false } }
    ],
    series: [
      { name: '订单数', type: 'bar', data: hourly.map((item) => Number(item.orders)), itemStyle: { color: '#3b82f6' } },
      {
        name: '营业额',
        type: 'line',
        yAxisIndex: 1,
        smooth: true,
        data: hourly.map((item) => Number(item.cost)),
        itemStyle: { color: '#22d3ee' },
        areaStyle: { color: 'rgba(34,211,238,0.12)' }
      }
    ]
  };
});

const parentOption = computed<EChartsOption>(() => {
  const rows = [...parentStats.value].reverse();
  return {
    tooltip: { trigger: 'axis' },
    grid: { top: 12, left: 12, right: 46, bottom: 12, containLabel: true },
    xAxis: { type: 'value', axisLabel: { color: '#7d8aa3' }, splitLine: { lineStyle: { color: '#243249' } } },
    yAxis: { type: 'category', data: rows.map((row) => row.parent_label), axisLabel: { color: '#a8b5cc' } },
    series: [
      {
        type: 'bar',
        barWidth: 16,
        data: rows.map((row, index) => ({
          value: Number(row.cnt),
          itemStyle: { color: ['#3b82f6', '#22d3ee', '#22c55e', '#f59e0b', '#a78bfa'][index % 5] }
        })),
        label: { show: true, position: 'right', color: '#a8b5cc', formatter: ({ value }) => formatNumber(value as number) }
      }
    ]
  };
});

const consumeOption = computed<EChartsOption>(() => pieOption(labelGroup('电视消费水平'), ['#3b82f6', '#22d3ee', '#22c55e', '#f59e0b']));
const brandOption = computed<EChartsOption>(() => pieOption(labelGroup('业务品牌'), ['#f59e0b', '#a78bfa', '#22d3ee', '#22c55e', '#ef4444']));

function pieOption(data: Array<{ name: string; value: number }>, colors: string[]): EChartsOption {
  return {
    tooltip: { trigger: 'item' },
    color: colors,
    series: [
      {
        type: 'pie',
        radius: ['42%', '68%'],
        center: ['50%', '54%'],
        data,
        label: { color: '#a8b5cc', fontSize: 11 },
        itemStyle: { borderColor: '#0f172a', borderWidth: 2 }
      }
    ]
  };
}

onMounted(() => {
  updateClock();
  loadRealtime();
  loadStats();
  clockTimer = window.setInterval(updateClock, 1000);
  realtimeTimer = window.setInterval(loadRealtime, 10000);
});

onBeforeUnmount(() => {
  if (clockTimer) window.clearInterval(clockTimer);
  if (realtimeTimer) window.clearInterval(realtimeTimer);
});
</script>
