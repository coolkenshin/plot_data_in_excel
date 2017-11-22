#!/bin/bash

#FILE_TO_FIND='nfs3.ops_size.txt'

usage()
{ 
    echo "Usage:"
    printf "\t%s" `basename $0`" dir_to_find"
    echo
    echo
    exit 1
}

################################
# AWK scripts                  #
################################
read -d '' scriptVariable << 'EOF'

#col_list = ['date_time', 'total_iops', 'io_size <1k', 'io_size >=1k', 'io_size >=2k', 'io_size >=4k',
#                'io_size >=8k', 'io_size >=16k', 'io_size >=32k', 'io_size >=64k', 'io_size >=128k', 
#                'io_size >=256k', 'io_size >=512k']
#    col_dict = dict(zip(col_list, range(0, len(col_list))))

function get_dict_index(x) {
        x = int(x / 1024)
        idx = 0
        while (x >= 1) {
            x = x / 2
            idx += 1
        }
        # index of io_size<1k
        idx += 2

        if (idx > 12)
            idx = 12
        return idx
}
BEGIN {
    cur_hour =""
    former_hour=""
    count = 0
}

NF == 5 && NR == 2 {
    split($1" "$2, arr, ":")
    cur_hour = arr[1]
    former_hour = arr[1]
}

NF == 5 {
    split($1" "$2, arr, ":")
    cur_hour = arr[1]
    #print cur_hour, former_hour, count
    if (cur_hour != former_hour) {
        printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\\n",
            former_hour,
            int(ops["total_ops_num"]/count+0.5),
            int(ops[2]/count+0.5),
            int(ops[3]/count+0.5),
            int(ops[4]/count+0.5),
            int(ops[5]/count+0.5),
            int(ops[6]/count+0.5),
            int(ops[7]/count+0.5),
            int(ops[8]/count+0.5),
            int(ops[9]/count+0.5),
            int(ops[10]/count+0.5),
            int(ops[11]/count+0.5),
            int(ops[12]/count+0.5),
            int(ops[13]/count+0.5)

        ops["total_ops_num"] = $3
        
        ops[2] = 0
        ops[3] = 0
        ops[4] = 0
        ops[5] = 0
        ops[6] = 0
        ops[7] = 0
        ops[8] = 0
        ops[9] = 0
        ops[10] = 0
        ops[11] = 0
    
        idx = get_dict_index($5)
        ops[idx] = $4
        former_hour = cur_hour

        count = 1
    } else {
        former_hour = cur_hour
        ops["date_time"] = cur_hour
        ops["total_ops_num"] += $3
        idx = get_dict_index(int($5))
        ops[idx] = $4
        count += 1  
    }

}

NF == 2 {
    former_hour = cur_hour
    idx = get_dict_index($2)
    ops[idx] += $1
}

END {
        printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\\n",
            former_hour,
            int(ops["total_ops_num"]/count+0.5),
            int(ops[2]/count+0.5),
            int(ops[3]/count+0.5),
            int(ops[4]/count+0.5),
            int(ops[5]/count+0.5),
            int(ops[6]/count+0.5),
            int(ops[7]/count+0.5),
            int(ops[8]/count+0.5),
            int(ops[9]/count+0.5),
            int(ops[10]/count+0.5),
            int(ops[11]/count+0.5),
            int(ops[12]/count+0.5),
            int(ops[13]/count+0.5)
}
EOF
################################
# End of AWK Scripts           #
################################

#set -x

if [ -n "$1" ]
then
    WORK_DIR="$1"
else
    echo "Error:directory is not specified."
    usage
fi

FILE_TO_FIND="$2"

cd $WORK_DIR

cat_raw_file="${FILE_TO_FIND}.raw_all"
TMP_FILE_LIST="${FILE_TO_FIND}.filelist"
final_result_file="${FILE_TO_FIND}.csv"
if [ -f $cat_raw_file ]; then
    rm -f $cat_raw_file
fi


find $WORK_DIR -name $FILE_TO_FIND > $TMP_FILE_LIST
new_file_list=$(cat $TMP_FILE_LIST |sort)
for file in `echo $new_file_list`
do
    cat $file >> $cat_raw_file
    # Avoid no end of line and concateate with the next file
    echo >> $cat_raw_file
done

# Pre-process to remove empty lines in place
sed -e '/^$/d' -i '' $cat_raw_file 
# Pre-process some empty values
perl -i -pe 's/- -$/0 0/g' ${cat_raw_file}

awk "$scriptVariable" ${cat_raw_file} > ${final_result_file}

rm -f $cat_raw_file
rm -f $TMP_FILE_LIST
