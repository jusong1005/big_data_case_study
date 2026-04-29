<template>
  <main class="app-shell profile-page">
    <header class="topbar">
      <div>
        <p class="eyebrow">User Portrait</p>
        <h1>广电大数据用户画像平台</h1>
      </div>
      <div class="topbar-actions">
        <RouterLink class="nav-link" to="/screen">实时大屏</RouterLink>
        <span class="status-pill">系统运行中</span>
        <span class="clock">{{ currentTime }}</span>
      </div>
    </header>

    <section class="metric-grid">
      <article class="metric-card">
        <span>用户总量</span>
        <strong>2,463,779</strong>
        <small>广州地区广电用户</small>
      </article>
      <article class="metric-card">
        <span>画像标签数</span>
        <strong>9</strong>
        <small>消费 / 业务 / 预测</small>
      </article>
      <article class="metric-card">
        <span>SVM 模型准确率</span>
        <strong>79.25%</strong>
        <small>AUC: 0.8223</small>
      </article>
    </section>

    <section class="query-panel">
      <div class="query-copy">
        <p class="section-kicker">Profile Query</p>
        <h2>用户画像查询</h2>
        <p>输入用户编号后查询标签图谱、画像雷达和详细标签。</p>
      </div>
      <div class="query-actions">
        <el-input
          v-model="userId"
          clearable
          size="large"
          placeholder="请输入用户 ID，如 2698243"
          @keyup.enter="loadProfile"
        />
        <el-button type="primary" size="large" :loading="profileLoading" @click="loadProfile">查询画像</el-button>
        <el-button size="large" @click="openLabelDetails">标签详情</el-button>
        <el-button size="large" @click="pickRandomUser">随机</el-button>
      </div>
      <div class="sample-row" v-if="sampleUsers.length">
        <span>示例用户</span>
        <button v-for="uid in sampleUsers" :key="uid" type="button" @click="selectUser(uid)">
          {{ uid }}
        </button>
      </div>
    </section>

    <section class="content-grid">
      <article class="panel graph-panel">
        <div class="panel-header">
          <div>
            <p class="section-kicker">Knowledge Graph</p>
            <h3>用户标签知识图谱</h3>
          </div>
          <span class="data-badge">ECharts</span>
        </div>
        <EChart :option="labelGraphOption" />
      </article>

      <article class="panel profile-panel">
        <div class="panel-header">
          <div>
            <p class="section-kicker">Profile Result</p>
            <h3>用户画像结果</h3>
          </div>
          <span class="data-badge" v-if="profileReady">已加载</span>
        </div>

        <el-empty v-if="!hasSearched" description="输入用户 ID 后查看画像" />
        <el-skeleton v-else-if="profileLoading" animated :rows="8" />
        <el-empty v-else-if="!profileReady" description="未找到该用户画像" />

        <div v-else class="profile-result">
          <div class="person-strip">
            <div class="avatar">U</div>
            <div>
              <strong>用户 {{ userId }}</strong>
              <span>广电宽带/电视用户</span>
            </div>
          </div>
          <div class="radar-box">
            <EChart :option="radarOption" />
          </div>
          <div class="tag-groups">
            <div v-for="name in profileData?.parentName" :key="name" class="tag-group">
              <h4>{{ name }}</h4>
              <div>
                <el-tag v-for="tag in tagValues(name)" :key="tag" effect="dark" round>{{ tag }}</el-tag>
              </div>
            </div>
          </div>
        </div>
      </article>
    </section>

    <el-dialog v-model="detailVisible" title="用户标签详情" width="760px">
      <el-table :data="labelDetails" stripe height="420">
        <el-table-column prop="phone_no" label="用户编号" width="160" />
        <el-table-column prop="parent_label" label="父级标签" width="180" />
        <el-table-column prop="label" label="标签" />
      </el-table>
    </el-dialog>
  </main>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue';
import { ElMessage } from 'element-plus';
import type { EChartsOption } from 'echarts';
import EChart from '@/components/EChart.vue';
import { fetchSampleUsers, fetchUserLabels, fetchUserProfile } from '@/api/dashboard';
import type { UserLabel, UserProfileData } from '@/types/dashboard';

const userId = ref('2698243');
const sampleUsers = ref<number[]>([]);
const profileData = ref<UserProfileData | null>(null);
const labelDetails = ref<UserLabel[]>([]);
const detailVisible = ref(false);
const profileLoading = ref(false);
const hasSearched = ref(false);
const currentTime = ref('');
let clockTimer: number | undefined;

const profileReady = computed(() => Boolean(profileData.value && !profileData.value.isNull));

function updateClock() {
  currentTime.value = new Date().toLocaleString('zh-CN', { hour12: false });
}

function validateUserId() {
  if (!/^\d+$/.test(userId.value.trim())) {
    ElMessage.warning('用户 ID 不能为空，并且必须是数字');
    return false;
  }
  return true;
}

function tagValues(name: string) {
  const values = profileData.value?.[name];
  return Array.isArray(values) && values.length ? values : ['无'];
}

async function loadProfile() {
  if (!validateUserId()) return;
  hasSearched.value = true;
  profileLoading.value = true;
  try {
    const result = await fetchUserProfile(userId.value.trim());
    profileData.value = normalizeProfile(result.data);
  } catch (error) {
    profileData.value = null;
    ElMessage.error('用户画像查询失败');
  } finally {
    profileLoading.value = false;
  }
}

async function openLabelDetails() {
  if (!validateUserId()) return;
  try {
    labelDetails.value = await fetchUserLabels(userId.value.trim());
    detailVisible.value = true;
  } catch (error) {
    ElMessage.error('标签详情查询失败');
  }
}

function selectUser(uid: number) {
  userId.value = String(uid);
  loadProfile();
}

function pickRandomUser() {
  if (!sampleUsers.value.length) return;
  const index = Math.floor(Math.random() * sampleUsers.value.length);
  selectUser(sampleUsers.value[index]);
}

function normalizeProfile(data: UserProfileData) {
  const next = { ...data };
  next.parentName?.forEach((name) => {
    const values = next[name];
    if (Array.isArray(values) && values.length === 0) {
      next[name] = ['无'];
    }
  });
  return next;
}

const labelGraphOption = computed<EChartsOption>(() => ({
  backgroundColor: 'transparent',
  animationDuration: 1200,
  series: [
    {
      type: 'graph',
      layout: 'force',
      roam: true,
      draggable: true,
      force: { repulsion: 220, gravity: 0.05, edgeLength: [70, 130] },
      label: { show: true, color: '#f8fafc', fontSize: 12, fontWeight: 600 },
      lineStyle: { color: '#42526d', width: 1.5, curveness: 0.16 },
      edgeSymbol: ['none', 'arrow'],
      edgeSymbolSize: [0, 6],
      data: [
        { name: '用户画像', symbolSize: 76, itemStyle: { color: '#2563eb' } },
        { name: '消费维度', symbolSize: 54, itemStyle: { color: '#06b6d4' } },
        { name: '业务维度', symbolSize: 54, itemStyle: { color: '#f59e0b' } },
        { name: '预测维度', symbolSize: 54, itemStyle: { color: '#22c55e' } },
        ...['消费内容', '电视消费水平', '宽带消费水平', '销售品名称'].map((name) => ({
          name,
          symbolSize: 36,
          itemStyle: { color: '#0e7490' }
        })),
        ...['宽带产品带宽', '业务品牌', '电视入网程度', '宽带入网程度'].map((name) => ({
          name,
          symbolSize: 36,
          itemStyle: { color: '#b45309' }
        })),
        { name: '用户是否挽留', symbolSize: 36, itemStyle: { color: '#16a34a' } }
      ],
      links: [
        ['用户画像', '消费维度'],
        ['用户画像', '业务维度'],
        ['用户画像', '预测维度'],
        ['消费维度', '消费内容'],
        ['消费维度', '电视消费水平'],
        ['消费维度', '宽带消费水平'],
        ['消费维度', '销售品名称'],
        ['业务维度', '宽带产品带宽'],
        ['业务维度', '业务品牌'],
        ['业务维度', '电视入网程度'],
        ['业务维度', '宽带入网程度'],
        ['预测维度', '用户是否挽留']
      ].map(([source, target]) => ({ source, target }))
    }
  ]
}));

const radarOption = computed<EChartsOption>(() => {
  const names = profileData.value?.parentName || [];
  const scoreMap: Record<string, number> = {
    高: 3,
    中: 2,
    低: 1,
    深度: 3,
    中度: 2,
    浅度: 1,
    是: 3,
    否: 1,
    电视: 2,
    宽带: 2,
    融合: 3,
    '200M及以上': 4,
    '100M': 3,
    '50M': 2,
    '20M': 1
  };
  const maxMap: Record<string, number> = {
    消费内容: 3,
    电视消费水平: 3,
    宽带消费水平: 3,
    宽带产品带宽: 4,
    用户是否挽留: 3,
    业务品牌: 4,
    电视入网程度: 3,
    宽带入网程度: 3,
    销售品名称: 4
  };
  const values = names.map((name) => {
    const tags = tagValues(name);
    const score = tags.reduce((max, tag) => Math.max(max, scoreMap[tag] || 0), 0);
    return score || (tags[0] === '无' ? 0 : 2);
  });
  return {
    radar: {
      indicator: names.map((name) => ({ name, max: maxMap[name] || 3 })),
      radius: '65%',
      axisName: { color: '#9fb0c9', fontSize: 11 },
      splitLine: { lineStyle: { color: '#243249' } },
      splitArea: { areaStyle: { color: ['rgba(34,211,238,0.04)', 'rgba(59,130,246,0.07)'] } },
      axisLine: { lineStyle: { color: '#243249' } }
    },
    series: [
      {
        type: 'radar',
        data: [
          {
            value: values,
            name: '用户画像',
            areaStyle: { color: 'rgba(34,211,238,0.18)' },
            lineStyle: { color: '#22d3ee', width: 2 },
            itemStyle: { color: '#22d3ee' }
          }
        ]
      }
    ]
  };
});

onMounted(async () => {
  updateClock();
  clockTimer = window.setInterval(updateClock, 1000);
  try {
    sampleUsers.value = await fetchSampleUsers();
  } catch (error) {
    ElMessage.warning('示例用户加载失败');
  }
});

onBeforeUnmount(() => {
  if (clockTimer) window.clearInterval(clockTimer);
});
</script>
