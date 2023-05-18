#/usr/bin/python3
import json
import pandas as pd
import os
import subprocess as sp
import sys

global io_patterns

# to download files from tb - command
sh_cmd = "for i in $(tb-cli ls /staging/simplebench3 | grep summary.json); do tb-cli get /staging/simplebench3/$i $i; done"

json_cols = """results_sample_fio_conf1_1_local_result_randread-16-32.json_global options_bs
results_sample_fio_conf1_1_local_result_randread-16-32.json_global options_direct
results_sample_fio_conf1_1_local_result_randread-16-32.json_global options_invalidate
results_sample_fio_conf1_1_local_result_randread-16-32.json_global options_iodepth
results_sample_fio_conf1_1_local_result_randread-16-32.json_global options_ioengine
results_sample_fio_conf1_1_local_result_randread-16-32.json_global options_log_avg_msec
results_sample_fio_conf1_1_local_result_randread-16-32.json_global options_loops
results_sample_fio_conf1_1_local_result_randread-16-32.json_global options_numjobs
results_sample_fio_conf1_1_local_result_randread-16-32.json_global options_random_distribution
results_sample_fio_conf1_1_local_result_randread-16-32.json_global options_runtime
results_sample_fio_conf1_1_local_result_randread-16-32.json_global options_rw
results_sample_fio_conf1_1_local_result_randread-16-32.json_global options_write_bw_log
results_sample_fio_conf1_1_local_result_randread-16-32.json_global options_write_iops_log
results_sample_fio_conf1_1_local_result_randread-16-32.json_global options_write_lat_log
results_sample_fio_conf1_1_local_result_randread-16-32.json_jobs_trim_bw_max
results_sample_fio_conf1_1_local_result_randread-16-32.json_jobs_trim_bw_mean
results_sample_fio_conf1_1_local_result_randread-16-32.json_jobs_trim_bw_min
results_sample_fio_conf1_1_local_result_randread-16-32.json_jobs_read_clat_ns_percentile_50.000000
results_sample_fio_conf1_1_local_result_randread-16-32.json_jobs_read_clat_ns_percentile_70.000000
results_sample_fio_conf1_1_local_result_randread-16-32.json_jobs_read_clat_ns_percentile_90.000000
results_sample_fio_conf1_1_local_result_randread-16-32.json_jobs_read_clat_ns_percentile_95.000000
results_sample_fio_conf1_1_local_result_randread-16-32.json_jobs_read_clat_ns_percentile_99.000000
results_sample_fio_conf1_1_local_result_randread-16-32.json_jobs_read_clat_ns_percentile_99.900000
results_sample_fio_conf1_1_local_result_randread-16-32.json_jobs_read_clat_ns_percentile_99.990000
results_sample_fio_conf1_1_local_result_randread-16-32.json_jobs_read_iops_max
results_sample_fio_conf1_1_local_result_randread-16-32.json_jobs_read_iops_min
results_sample_fio_conf1_1_local_result_randread-16-32.json_jobs_read_iops_mean"""

def columns_prep(df_columns):
    final_cols = {}
    for col in df_columns:
        if 'global options' in col:
            final_cols[col] = 'config info ' + str(col.split('_')[-1])
        elif '_bw_' in col:
            final_cols[col] = 'BW ' + str(col.split('_')[-1])
        elif 'clat_ns_percentile' in col:
            final_cols[col] = 'P{} latency'.format(str(col.split('_')[-1]).split('.')[0])
        elif '_iops_' in col:
            final_cols[col] = 'IOps ' + str(col.split('_')[-1])
    return final_cols


def flatten_data(file):
    with open(file) as f:
        data = json.load(f)

        io_local = list(data["results"]["sample_fio_conf1"]["1"]["local"]["result"].keys())
        for each in io_local:
            if each not in io_patterns:
                io_patterns.append(each)

        def flatten_dict(d, sep = '_'):
            [flat_dict] = pd.json_normalize(d, sep=sep).to_dict(orient='records')
            return flat_dict

        d = flatten_dict(data)

        for k, v in d.items():
            if isinstance(v, list):
                if isinstance(v[0], dict):
                    d_flatinside = flatten_dict(v[0])
                    d[k] = d_flatinside
                    
                else:
                    continue
        dd = flatten_dict(d)
        final = flatten_dict(dd)
        return final
    return d


if __name__=="__main__":
    test_types = ["ncu", "stream", "specjbb2015", "speccpu2017", "fio"]
    # Add supported test types here after parsing logic is in place below.
    supported_test_types = ["stream"]
    path_to_json = '/Users/vnellu1/benchmarkfiles/2023-05-18'
    try:
        os.mkdir(path_to_json)
    except FileExistsError:
        pass
    os.chdir(path_to_json)
    sp.run(sh_cmd, shell=True)
    json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
    io_patterns = []
    ddc = []

    for json_f in json_files:
        l = {}
        file_path = os.path.join(path_to_json, json_f)
        test_type = ""
        for t in test_types:
            if "{}_summary".format(t) in file_path:
                test_type = t

        if test_type not in supported_test_types:
            sys.exit("only the following test types are supported with this script at present: {}".format(supported_test_types))

        if test_type == 'fio':
            l = flatten_data(file_path)
            l['file_name'] = file_path
            ddc.append(l)

    randreadcols =  json_cols.replace('\t', '\n').split('\n')
    read_cols = [x.replace('randread-16-32.json', 'read-11-8.json') for x in randreadcols]
    write_cols = [x.replace('randread-16-32.json', 'write-3-8.json').replace('_read_','_write_') for x in randreadcols]

    df = pd.DataFrame(ddc)
    df.to_csv(os.path.join(path_to_json,'benchmark_local_test_new.csv'), index=False)

    df.T.to_csv(os.path.join(path_to_json,'benchmark_local_test_new_t.csv'))

    #Function to extract the dataset
    df1 = df[randreadcols]
    df2 = df[read_cols]
    df3 = df[write_cols]

    df1_collist = df1.columns.values.tolist()
    newcols = columns_prep(df1_collist)
    df1.rename(newcols, axis='columns', inplace=True)
    df1.loc[:, 'IO pattern_operation'] =df1_collist[0].split('_')[7]
    
    df2_collist = df2.columns.values.tolist()
    newcols = columns_prep(df2_collist)
    df2.rename(newcols, axis='columns', inplace=True)
    df2.loc[:, 'IO pattern_operation'] =df2_collist[0].split('_')[7]
    
    df3_collist = df3.columns.values.tolist()
    newcols = columns_prep(df3_collist)
    df3.rename(newcols, axis='columns', inplace=True)
    df3.loc[:,'IO pattern_operation'] =df3_collist[0].split('_')[7]

    #contact all dataframes
    final_df = pd.concat([df1, df2, df3])

    #Save Final CSV
    final_df.dropna().to_csv(os.path.join(path_to_json,"final.csv"), index=False)