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
        printf "%s,%s,%s,%s\n",
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
        printf "%s,%s,%s,%s\n",
            former_hour,
            int(ops["total_ops_num"]/count+0.5),
            ops["max"],
            ops["min"]
    }
}