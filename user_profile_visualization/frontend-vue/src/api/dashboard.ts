import { apiClient } from './client';
import type { ApiResult, LabelStat, ParentStat, RealtimeData, UserLabel, UserProfileData } from '@/types/dashboard';

export async function fetchSampleUsers() {
  const { data } = await apiClient.get<number[]>('/labels/samples');
  return data;
}

export async function fetchUserLabels(userId: string | number) {
  const { data } = await apiClient.get<UserLabel[]>(`/labels/user/${userId}`);
  return data;
}

export async function fetchUserProfile(userId: string | number) {
  const { data } = await apiClient.get<ApiResult<UserProfileData>>(`/labels/users/${userId}/all/`);
  return data;
}

export async function fetchRealtimeData() {
  const { data } = await apiClient.get<RealtimeData>('/screen/realtime');
  return data;
}

export async function fetchLabelStats() {
  const { data } = await apiClient.get<LabelStat[]>('/screen/label-stats');
  return data;
}

export async function fetchParentStats() {
  const { data } = await apiClient.get<ParentStat[]>('/screen/parent-stats');
  return data;
}
