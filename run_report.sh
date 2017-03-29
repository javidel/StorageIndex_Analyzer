t_name=$1
perc=$2
table_path=$(hive -S -e "describe formatted $1 ;" | grep 'Location' | awk '{ print $NF }')
table_delimitator=$(hive -S -e "describe formatted $1 ;" | grep 'serialization.format' | awk '{ print $NF }')
n_cols=$(hive -S -e "describe $1" | wc -l;)
data_types=$(hive -S -e "describe $1"  | awk '{print $2}')
column_name=$(hive -S -e "describe $1"  | awk '{print $1}')
spark-submit main_report.py $table_path $n_cols "$data_types" $table_delimitator "$column_name" $perc
