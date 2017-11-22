#!/bin/bash

#FILE_TO_FIND='nfs3.ops.txt'

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
BEGIN {
    cur_hour =""
    former_hour=""
    count = 0
    ops["max"] = 0
    ops["min"] = 9999999999
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
        printf "%s,%s,%s,%s\\n",
            former_hour,
            int(ops["total_ops_num"]/count+0.5),
            ops["max"],
            ops["min"]
        
        # New line values
        ops["total_ops_num"] = $3
        ops["max"] = $3
        ops["min"] = $3
        former_hour = cur_hour

        count = 1
    } else {
        former_hour = cur_hour
        ops["date_time"] = cur_hour
        ops["total_ops_num"] += $3
        if ($3 > ops["max"])
            ops["max"] = $3
        if ($3 < ops["min"])
            ops["min"] = $3
        count += 1  
    }

}

END {
    if (cur_hour != former_hour) {
        printf "%s,%s,%s,%s\\n",
            former_hour,
            int(ops["total_ops_num"]/count+0.5),
            ops["max"],
            ops["min"]
    }
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
