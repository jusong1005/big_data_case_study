export interface UserLabel {
  phone_no: number;
  label: string;
  parent_label: string;
}

export interface ApiResult<T> {
  message: string;
  data: T;
  status: 'SUCCESS' | 'FAIL';
}

export interface UserProfileData {
  parentName: string[];
  isNull: boolean;
  [key: string]: string[] | string | boolean | string[];
}

export interface RealtimePoint {
  hour: string;
  orders: string;
  cost: string;
}

export interface RealtimeData {
  totalOrders?: string;
  totalcost?: string;
  totalValidOrders?: string;
  increase_cost?: string;
  increase_order?: string;
  hourly?: RealtimePoint[];
  error?: string;
}

export interface LabelStat {
  parent_label: string;
  label: string;
  cnt: number;
}

export interface ParentStat {
  parent_label: string;
  cnt: number;
}
