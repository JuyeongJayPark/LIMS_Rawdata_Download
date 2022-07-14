import os
import pandas as pd 
import argparse 

parser = argparse.ArgumentParser()
parser.add_argument('-u', '--user', default="jypark")
args = parser.parse_args()


user = args.user

cwd = os.getcwd()
Files = os.listdir(cwd)
NIPTON_Files = [File for File in Files if File.startswith("NIPTON")]
NIPTON_File = NIPTON_Files[0]

NIPTON_df = pd.read_csv(NIPTON_File, header=0, skiprows=[1])

NIPTON_df_essential = NIPTON_df[['대상정보','데이터']]

TN_list = NIPTON_df_essential['대상정보'].tolist()

output_bash = open('rawdata_download.sh', 'w')

for TN_ID in TN_list:
    output_bash.write(f"mkdir {TN_ID}\n")
    run_info_object = NIPTON_df_essential[NIPTON_df_essential['대상정보'] == TN_ID]['데이터'].str.split("\n").tolist()
    run_info_list = sum(run_info_object,[])
    for run_info in run_info_list:
        run_info_path = run_info.split(":")[1]
        output_bash.write(f"rsync -PLrvh --progress --partial {user}@qc1:{run_info_path}/{TN_ID}* ./{TN_ID}\n")
