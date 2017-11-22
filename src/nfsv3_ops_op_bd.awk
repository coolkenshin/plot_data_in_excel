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
		printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
			former_hour,
			int(ops["total_ops_num"]/count),
			int(ops["access"]/count),
			int(ops["commit"]/count),
			int(ops["create"]/count),
			int(ops["fsinfo"]/count),
			int(ops["fsstat"]/count),
			int(ops["getattr"]/count),
			int(ops["link"]/count),
			int(ops["lookup"]/count),
			int(ops["mkdir"]/count),
			int(ops["mknod"]/count),
			int(ops["pathconf"]/count),
			int(ops["read"]/count),
			int(ops["readdir"]/count),
			int(ops["readdirplus"]/count),
			int(ops["readlink"]/count),
			int(ops["remove"]/count),
			int(ops["rename"]/count),
			int(ops["rmdir"]/count),
			int(ops["setattr"]/count),
			int(ops["symlink"]/count),
			int(ops["write"]/count)
		
		ops["total_ops_num"] = $3
		ops["access"] = 0
		ops["commit"] = 0
		ops["create"] = 0
		ops["fsinfo"] = 0
		ops["fsstat"] = 0
		ops["getattr"] = 0
		ops["link"] = 0
		ops["lookup"] = 0
		ops["mkdir"] = 0
		ops["mknod"] = 0
		ops["pathconf"] = 0
		ops["read"] = 0
		ops["readdir"] = 0
		ops["readdirplus"] = 0
		ops["readlink"] = 0
		ops["remove"] = 0
		ops["rename"] = 0
		ops["rmdir"] = 0
		ops["setattr"] = 0
		ops["symlink"] = 0
		ops["write"] = 0
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
	printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
		former_hour,
		int(ops["total_ops_num"]/count),
		int(ops["access"]/count),
		int(ops["commit"]/count),
		int(ops["create"]/count),
		int(ops["fsinfo"]/count),
		int(ops["fsstat"]/count),
		int(ops["getattr"]/count),
		int(ops["link"]/count),
		int(ops["lookup"]/count),
		int(ops["mkdir"]/count),
		int(ops["mknod"]/count),
		int(ops["pathconf"]/count),
		int(ops["read"]/count),
		int(ops["readdir"]/count),
		int(ops["readdirplus"]/count),
		int(ops["readlink"]/count),
		int(ops["remove"]/count),
		int(ops["rename"]/count),
		int(ops["rmdir"]/count),
		int(ops["setattr"]/count),
		int(ops["symlink"]/count),
		int(ops["write"]/count)
}