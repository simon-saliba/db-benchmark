#!/usr/bin/env python
from _helpers.helpers import memory_usage, write_log, make_chk

print("# groupby-terality.py", flush=True)

import os
import gc
import sys
import timeit
import terality as pd

exec(open("./_helpers/helpers.py").read())

ver = pd.__version__
git = "unset"
task = "groupby"
solution = "terality"
fun = ".groupby"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ['SRC_DATANAME']
src_grp = os.path.join("data", data_name + ".csv")
print("loading dataset %s" % data_name, flush=True)

na_flag = int(data_name.split("_")[3])
if na_flag > 0:
    # x = pd.read_csv(src_grp, dtype={'id1':'category','id2':'category','id3':'category','id4':'Int32','id5':'Int32','id6':'Int32','v1':'Int32','v2':'Int32','v3':'float64'})
    print("skip due to na_flag>0: #171", flush=True, file=sys.stderr)
    exit(0)  # not yet implemented #171

from datatable import fread  # for loading data only, see #47

x = pd.DataFrame.from_pandas(fread(src_grp, na_strings=['']).to_pandas())
# x['id1'] = x['id1'].astype('category') # remove after datatable#1691
# x['id2'] = x['id2'].astype('category')
# x['id3'] = x['id3'].astype('category')
x['id4'] = x['id4'].astype('Int32')  ## NA-aware types improved after h2oai/datatable#2761 resolved
x['id5'] = x['id5'].astype('Int32')
x['id6'] = x['id6'].astype('Int32')
x['v1'] = x['v1'].astype('Int32')
x['v2'] = x['v2'].astype('Int32')
x['v3'] = x['v3'].astype('float64')

print(len(x.index), flush=True)

task_init = timeit.default_timer()
print("grouping...", flush=True)

# question = "sum v1 by id1" # q1
# print(question)
# gc.collect()
# t_start = timeit.default_timer()
# ans = x.groupby('id1', as_index=False, sort=False, observed=True, dropna=False)['v1'].sum()
# print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v1'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# del ans
# gc.collect()
# t_start = timeit.default_timer()
# ans = x.groupby('id1', as_index=False, sort=False, observed=True, dropna=False)['v1'].sum()
# print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v1'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
# del ans
#
# question = "sum v1 by id1:id2" # q2
# print(question)
# gc.collect()
# t_start = timeit.default_timer()
# ans = x.groupby(['id1','id2'], as_index=False, sort=False, observed=True, dropna=False)['v1'].sum()
# print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v1'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# del ans
# gc.collect()
# t_start = timeit.default_timer()
# ans = x.groupby(['id1','id2'], as_index=False, sort=False, observed=True, dropna=False)['v1'].sum()
# print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v1'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
# del ans
#
# question = "sum v1 mean v3 by id3" # q3
# print(question)
# gc.collect()
# t_start = timeit.default_timer()
# group_by = x.groupby('id3', as_index=False, sort=False, observed=True, dropna=False)
# v1 = group_by['v1'].sum()
# v3 = group_by['v3'].mean()
# ans = pd.merge(v1, v3, on='id3')
# print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v1'].sum(), ans['v3'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# del ans
# gc.collect()
# t_start = timeit.default_timer()
# group_by = x.groupby('id3', as_index=False, sort=False, observed=True, dropna=False)
# v1 = group_by['v1'].sum()
# v3 = group_by['v3'].mean()
# ans = pd.merge(v1, v3, on='id3')
# print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v1'].sum(), ans['v3'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
# del ans
#
# question = "mean v1:v3 by id4" # q4
# print(question)
# gc.collect()
# t_start = timeit.default_timer()
# group_by = x.groupby('id4', as_index=False, sort=False, observed=True, dropna=False)
# v1 = group_by['v1'].mean()
# v2 = group_by['v2'].mean()
# v3 = group_by['v3'].mean()
# ans = v1
# ans['v2'] = v2['v2']
# ans['v3'] = v3['v3']
# print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# del ans
# gc.collect()
# t_start = timeit.default_timer()
# group_by = x.groupby('id4', as_index=False, sort=False, observed=True, dropna=False)
# v1 = group_by['v1'].mean()
# v2 = group_by['v2'].mean()
# v3 = group_by['v3'].mean()
# ans = v1
# ans['v2'] = v2['v2']
# ans['v3'] = v3['v3']
# print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
# del ans
#
# question = "sum v1:v3 by id6" # q5
# print(question)
# gc.collect()
# t_start = timeit.default_timer()
# group_by = x.groupby('id6', as_index=False, sort=False, observed=True, dropna=False)
# v1 = group_by['v1'].sum()
# v2 = group_by['v2'].sum()
# v3 = group_by['v3'].sum()
# ans = v1
# ans['v2'] = v2['v2']
# ans['v3'] = v3['v3']
# print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# del ans
# gc.collect()
# t_start = timeit.default_timer()
# group_by = x.groupby('id6', as_index=False, sort=False, observed=True, dropna=False)
# v1 = group_by['v1'].sum()
# v2 = group_by['v2'].sum()
# v3 = group_by['v3'].sum()
# ans = v1
# ans['v2'] = v2['v2']
# ans['v3'] = v3['v3']
# print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
# del ans
#
# question = "median v3 sd v3 by id4 id5" # q6
# print(question)
# gc.collect()
# t_start = timeit.default_timer()
# group_by = x.groupby(['id4', 'id5'], as_index=False, sort=False, observed=True, dropna=False)
# ans_median = group_by['v3'].median()
# ans_std = group_by['v3'].std()
# print(ans_median.shape, flush=True)
# print(ans_std.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans_median['v3'].sum(), ans_std['v3'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans_median.shape[0], out_cols=ans_median.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# del ans_median
# del ans_std
# gc.collect()
# t_start = timeit.default_timer()
# group_by = x.groupby(['id4', 'id5'], as_index=False, sort=False, observed=True, dropna=False)
# ans_median = group_by['v3'].median()
# ans_std = group_by['v3'].std()
# print(ans_median.shape, flush=True)
# print(ans_std.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans_median['v3'].sum(), ans_std['v3'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans_median.shape[0], out_cols=ans_median.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# print("median")
# print(ans_median.head(3), flush=True)
# print(ans_median.tail(3), flush=True)
# print("std")
# print(ans_std.head(3), flush=True)
# print(ans_std.tail(3), flush=True)
# del ans_median
# del ans_std
#
# question = "max v1 - min v2 by id3" # q7
# print(question)
# gc.collect()
# t_start = timeit.default_timer()
# groupby = x.groupby('id3', as_index=False, sort=False, observed=True, dropna=False)
# v1 = groupby['v1'].max()
# v2 = groupby['v2'].min()
# ans = v1
# ans['v2'] = v2['v2']
# ans = ans.assign(range_v1_v2=lambda x: x['v1']-x['v2'])[['id3','range_v1_v2']]
# print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['range_v1_v2'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# del ans
# gc.collect()
# t_start = timeit.default_timer()
# groupby = x.groupby('id3', as_index=False, sort=False, observed=True, dropna=False)
# v1 = groupby['v1'].max()
# v2 = groupby['v2'].min()
# ans = v1
# ans['v2'] = v2['v2']
# ans = ans.assign(range_v1_v2=lambda x: x['v1']-x['v2'])[['id3','range_v1_v2']]
# print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['range_v1_v2'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
# del ans
#
# question = "largest two v3 by id6" # q8
# print(question)
# print("groupby.head not implemented by terality")
# gc.collect()
# t_start = timeit.default_timer()
# ans = x[~x['v3'].isna()][['id6','v3']].sort_values('v3', ascending=False).groupby('id6', as_index=False, sort=False, observed=True, dropna=False).head(2)
# ans.reset_index(drop=True, inplace=True)
# print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v3'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# del ans
# gc.collect()
# t_start = timeit.default_timer()
# ans = x[~x['v3'].isna()][['id6','v3']].sort_values('v3', ascending=False).groupby('id6', as_index=False, sort=False, observed=True, dropna=False).head(2)
# ans.reset_index(drop=True, inplace=True)
# print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['v3'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
# del ans

# question = "regression v1 v2 by id2 id4" # q9
##corr().iloc[0::2][['v2']]**2 # on 1e8,k=1e2 slower, 76s vs 47s
# gc.collect()
# t_start = timeit.default_timer()
# ans = x[['id2','id4','v1','v2']].groupby(['id2','id4'], as_index=False, sort=False, observed=True, dropna=False).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}))
# print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['r2'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# del ans
# gc.collect()
# t_start = timeit.default_timer()
# ans = x[['id2','id4','v1','v2']].groupby(['id2','id4'], as_index=False, sort=False, observed=True, dropna=False).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}))
# print(ans.shape, flush=True)
# t = timeit.default_timer() - t_start
# m = memory_usage()
# t_start = timeit.default_timer()
# chk = [ans['r2'].sum()]
# chkt = timeit.default_timer() - t_start
# write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
# print(ans.head(3), flush=True)
# print(ans.tail(3), flush=True)
# del ans

question = "sum v3 count by id1:id6"  # q10
print(question)
gc.collect()
t_start = timeit.default_timer()
groupby = x.groupby(['id1', 'id2', 'id3', 'id4', 'id5', 'id6'], as_index=False, sort=False, observed=True, dropna=False)
v3 = groupby['v3'].sum()
# Changing 'size' to 'count`, 'size' not implemented by Terality
# v1 = groupby['v1'].count()
ans = v3
# ans['v1'] = v1['v1']
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v3'].sum()]
# chk = [ans['v3'].sum(), ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0],
          out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m,
          cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
groupby = x.groupby(['id1', 'id2', 'id3', 'id4', 'id5', 'id6'], as_index=False, sort=False, observed=True, dropna=False)
v3 = groupby['v3'].sum()
# Changing 'size' to 'count`, 'size' not implemented by Terality
# v1 = groupby['v1'].count()
ans = v3
# ans['v1'] = v1['v1']
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v3'].sum()]
# chk = [ans['v3'].sum(), ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0],
          out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m,
          cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

print("grouping finished, took %0.fs" % (timeit.default_timer() - task_init), flush=True)

exit(0)
