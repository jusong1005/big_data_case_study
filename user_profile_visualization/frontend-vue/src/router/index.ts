import { createRouter, createWebHistory } from 'vue-router';
import ProfileDashboard from '@/pages/ProfileDashboard.vue';
import RealtimeScreen from '@/pages/RealtimeScreen.vue';

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'profile', component: ProfileDashboard },
    { path: '/screen', name: 'screen', component: RealtimeScreen }
  ]
});

export default router;
