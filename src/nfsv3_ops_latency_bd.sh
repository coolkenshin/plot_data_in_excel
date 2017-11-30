#!/bin/bash

#FILE_TO_FIND='nfs3.ops_latency.txt'

usage()
{ 
    echo "Usage:"
	printf "\t%s" `basename $0`" dir_to_find file_to_find"
    echo
    echo
    exit 1
}

################################
# AWK scripts                  #
################################
read -d '' scriptVariable << 'EOF'

#col_list = ['date_time', 'total_iops', 'latency <10μs', 'latency >=10μs', 'latency >=100μs', 'latency >=1ms',
#                'latency >=10ms', 'latency >=100ms', 'latency >=1s', 'latency >=10s'] 
#    col_dict = dict(zip(col_list, range(0, len(col_list))))

function get_dict_index(x) {
	if (x<10) {
    	return 2 
	} else if (x<100) {
		return 3
	} else if (x<1000) {
		return 4
	} else if (x<10000) {
		return 5
	} else if (x<100000) {
		return 6
	} else if (x<1000000) {
		return 7
	} else if (x<10000000) {
		return 8
	} else {
		return 9
	}
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
        printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\\n",
            former_hour,
            int(ops["total_ops_num"]/count+0.5),
            int(ops[2]/count+0.5),
            int(ops[3]/count+0.5),
            int(ops[4]/count+0.5),
            int(ops[5]/count+0.5),
            int(ops[6]/count+0.5),
            int(ops[7]/count+0.5),
            int(ops[8]/count+0.5),
            int(ops[9]/count+0.5)

        ops["total_ops_num"] = $3
        ops[2] = 0
        ops[3] = 0
        ops[4] = 0
        ops[5] = 0
        ops[6] = 0
        ops[7] = 0
        ops[8] = 0
        ops[9] = 0
    
        idx = get_dict_index($5)
        ops[idx] = $4
        former_hour = cur_hour

        count = 1
    } else {
        former_hour = cur_hour
        ops["date_time"] = cur_hour
        ops["total_ops_num"] += $3
        idx = get_dict_index(int($5))
        ops[idx] += $4
        count += 1
    }

}

NF == 2 {
    former_hour = cur_hour
    idx = get_dict_index($2)
    ops[idx] += $1
}

END {
	printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\\n",
        former_hour,
        int(ops["total_ops_num"]/count+0.5),
        int(ops[2]/count+0.5),
        int(ops[3]/count+0.5),
	    int(ops[4]/count+0.5),
        int(ops[5]/count+0.5),
        int(ops[6]/count+0.5),
        int(ops[7]/count+0.5),
        int(ops[8]/count+0.5),
        int(ops[9]/count+0.5)
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
