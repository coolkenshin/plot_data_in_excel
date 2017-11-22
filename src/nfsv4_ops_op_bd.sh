#!/bin/bash

#FILE_TO_FIND='nfs4.ops_op.txt'

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
        printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\\n",
            former_hour,
            int(ops["total_ops_num"]/count+0.5),
            int(ops["access"]/count+0.5),
            int(ops["close"]/count+0.5),
            int(ops["commit"]/count+0.5),
            int(ops["create"]/count+0.5),
            int(ops["delegpurge"]/count+0.5),
            int(ops["delegreturn"]/count+0.5),
            int(ops["getattr"]/count+0.5),
            int(ops["getfh"]/count+0.5),
            int(ops["link"]/count+0.5),
            int(ops["lock"]/count+0.5),
            int(ops["lockt"]/count+0.5),
            int(ops["locku"]/count+0.5),
            int(ops["lookup"]/count+0.5),
            int(ops["lookupp"]/count+0.5),
            int(ops["nverify"]/count+0.5),
            int(ops["open"]/count+0.5),
            int(ops["openattr"]/count+0.5),
            int(ops["open-confirm"]/count+0.5),
            int(ops["open-downgrade"]/count+0.5),
            int(ops["putfh"]/count+0.5),
            int(ops["putpubfh"]/count+0.5),
            int(ops["putrootfh"]/count+0.5),
            int(ops["read"]/count+0.5),
            int(ops["readdir"]/count+0.5),
            int(ops["readlink"]/count+0.5),
            int(ops["remove"]/count+0.5),
            int(ops["rename"]/count+0.5),
            int(ops["renew"]/count+0.5),
            int(ops["restorefh"]/count+0.5),
            int(ops["savefh"]/count+0.5),
            int(ops["secinfo"]/count+0.5),
            int(ops["setattr"]/count+0.5),
            int(ops["setclientid"]/count+0.5),
            int(ops["setclientid-confirm"]/count+0.5),
            int(ops["verify"]/count+0.5),
            int(ops["write"]/count+0.5),
            int(ops["release-lockowner"]/count+0.5)
        
        ops["total_ops_num"] = $3
        ops["access"] = 0
        ops["close"] = 0
        ops["commit"] = 0
        ops["create"] = 0
        ops["delegpurge"] = 0
        ops["delegreturn"] = 0
        ops["getattr"] = 0
        ops["getfh"] = 0
        ops["link"] = 0
        ops["lock"] = 0
        ops["lockt"] = 0
        ops["locku"] = 0
        ops["lookup"] = 0
        ops["lookupp"] = 0
        ops["nverify"] = 0
        ops["open"] = 0
        ops["openattr"] = 0
        ops["open-confirm"] = 0
        ops["open-downgrade"] = 0
        ops["putfh"] = 0
        ops["putpubfh"] = 0
        ops["putrootfh"] = 0
        ops["read"] = 0
        ops["readdir"] = 0
        ops["readlink"] = 0
        ops["remove"] = 0
        ops["rename"] = 0
        ops["renew"] = 0
        ops["restorefh"] = 0
        ops["savefh"] = 0
        ops["secinfo"] = 0
        ops["setattr"] = 0
        ops["setclientid"] = 0
        ops["setclientid-confirm"] = 0
        ops["verify"] = 0
        ops["write"] = 0
        ops["release-lockowner"] = 0
        ops[$5] = $4
        former_hour = cur_hour

        count = 1
    } else {
        former_hour = cur_hour
        ops["date_time"] = cur_hour
        ops["total_ops_num"] += $3
        ops[$5] += $4
        count += 1  
    }

}

NF == 2 {
    former_hour = cur_hour
    ops[$2] += $1
}

END {
    printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\\n",
        former_hour,
        int(ops["total_ops_num"]/count+0.5),
        int(ops["access"]/count+0.5),
        int(ops["close"]/count+0.5),
        int(ops["commit"]/count+0.5),
        int(ops["create"]/count+0.5),
        int(ops["delegpurge"]/count+0.5),
        int(ops["delegreturn"]/count+0.5),
        int(ops["getattr"]/count+0.5),
        int(ops["getfh"]/count+0.5),
        int(ops["link"]/count+0.5),
        int(ops["lock"]/count+0.5),
        int(ops["lockt"]/count+0.5),
        int(ops["locku"]/count+0.5),
        int(ops["lookup"]/count+0.5),
        int(ops["lookupp"]/count+0.5),
        int(ops["nverify"]/count+0.5),
        int(ops["open"]/count+0.5),
        int(ops["openattr"]/count+0.5),
        int(ops["open-confirm"]/count+0.5),
        int(ops["open-downgrade"]/count+0.5),
        int(ops["putfh"]/count+0.5),
        int(ops["putpubfh"]/count+0.5),
        int(ops["putrootfh"]/count+0.5),
        int(ops["read"]/count+0.5),
        int(ops["readdir"]/count+0.5),
        int(ops["readlink"]/count+0.5),
        int(ops["remove"]/count+0.5),
        int(ops["rename"]/count+0.5),
        int(ops["renew"]/count+0.5),
        int(ops["restorefh"]/count+0.5),
        int(ops["savefh"]/count+0.5),
        int(ops["secinfo"]/count+0.5),
        int(ops["setattr"]/count+0.5),
        int(ops["setclientid"]/count+0.5),
        int(ops["setclientid-confirm"]/count+0.5),
        int(ops["verify"]/count+0.5),
        int(ops["write"]/count+0.5),
        int(ops["release-lockowner"]/count+0.5)
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
