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
        printf "%s,%s,%s,%s\n",
            former_hour,
            int(ops["total_ops_num"]/count+0.5),
            int(ops["write"]/count+0.5),
            int(ops["read"]/count+0.5)
        
        ops["total_ops_num"] = $3
        ops["write"] = 0
        ops["read"] = 0
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
        printf "%s,%s,%s,%s\n",
            former_hour,
            int(ops["total_ops_num"]/count+0.5),
            int(ops["write"]/count+0.5),
            int(ops["read"]/count+0.5)
}