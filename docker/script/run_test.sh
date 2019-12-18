#!/bin/bash

MntPoint=/cfs/mnt
mkdir -p /cfs/bin /cfs/log /cfs/mnt
src_path=/go/src/github.com/chubaofs/cfs

Master1Addr="192.168.0.11:17010"
LeaderAddr=""
VolName=ltptest
Owner=ltptest
AuthKey="0e20229116d5a9a4a9e876806b514a85"

getLeaderAddr() {
    echo -n "check Master "
    for i in $(seq 1 300) ; do
        LeaderAddr=$(curl -s "http://$Master1Addr/admin/getCluster" | jq '.data.LeaderAddr' | tr -d '"' )
        if [ "x$LeaderAddr" != "x" ] ; then
            break
        fi
        echo -n "."
        sleep 1
    done
    if [ "x$LeaderAddr" == "x" ] ; then
        echo -n "timeout, exit\n"
        exit 1
    fi
    echo "ok"
}

check_status() {
    node="$1"
    up=0
    echo -n "check $node "
    for i in $(seq 1 300) ; do
        clusterInfo=$(curl -s "http://$LeaderAddr/admin/getCluster")
        #NodeUpCount=$( echo "$clusterInfo" | jq ".data.${node}s | .[].Status" | grep "true" | wc -l)
        NodeTotoalGB=$( echo "$clusterInfo" | jq ".data.${node}StatInfo.TotalGB" )
        if [[ $NodeTotoalGB -gt 0 ]]  ; then
            up=1
            break
        fi
        echo -n "."
        sleep 1
    done
    if [ $up -eq 0 ] ; then
        echo -n "timeout, exit"
        curl "http://$LeaderAddr/admin/getCluster" | jq
        exit 1
    fi
    echo "ok"
}


create_vol() {
    echo -n "create vol "
    res=$(curl -s "http://$LeaderAddr/admin/createVol?name=$VolName&capacity=30&owner=$Owner")
    code=$(echo "$res" | jq .code)
    if [[ $code -ne 0 ]] ; then
        echo " failed, exit"
        curl -s "http://$LeaderAddr/admin/getCluster" | jq
        exit 1
    fi
    echo "ok"
}

create_dp() {
    echo -n "create datapartition "
    res=$(curl -s "http://$LeaderAddr/dataPartition/create?count=20&name=$VolName&type=extent" )
    code=$(echo "$res" | jq .code)
    if [[ $code -ne 0 ]] ; then
        echo " failed, exit"
        curl -s "http://$LeaderAddr/admin/getCluster" | jq
        exit 1
    fi
    echo "ok"
}

print_error_info() {
    echo "------ err ----"
    cat /cfs/log/cfs.out
    cat /cfs/log/client/client_info.log
    cat /cfs/log/client/client_error.log
    cat /cfs/log/client/client_warn.log
    curl -s "http://$LeaderAddr/admin/getCluster" | jq
    mount
    df -h
    stat $MntPoint
    ls -l $MntPoint
    ls -l $LTPTestDir
}

start_client() {
    echo -n "start client "
    nohup /cfs/bin/cfs-client -c /cfs/conf/client.json >/cfs/log/cfs.out 2>&1 &
    sleep 10
    res=$( stat $MntPoint | grep -q "Inode: 1" ; echo $? )
    if [[ $res -ne 0 ]] ; then
        echo "failed"
        print_error_info
        exit $res
    fi

    echo "ok"
}

wait_proc_done() {
    proc_name=$1
    pid=$( ps -ef | grep "$proc_name" | grep -v "grep" | awk '{print $2}' )
    logfile=$2
    logfile2=${logfile}-2
    logfile3=${logfile}-3
    maxtime=${3:-3000}
    checktime=${4:-60}
    retfile=${5:-"/tmp/ltpret"}
    timeout=1
    pout=0
    lastlog=""
    for i in $(seq 1 $maxtime) ; do
        if ! `ps -ef  | grep -v "grep" | grep -q "$proc_name" ` ; then
            echo "$proc_name run done"
            timeout=0
            break
        fi
        sleep 1
        ((pout+=1))
        if [ $(cat $logfile | wc -l) -gt 0  ] ; then
            pout=0
            cat $logfile > $logfile2 && cat $logfile2 >> $logfile3 && > $logfile
            cat $logfile2 && rm -f $logfile2
        fi
        if [[ $pout -ge $checktime ]] ; then
            echo -n "."
            pout=0
        fi
    done
    if [[ $timeout -eq 1 ]] ;then
        echo "$proc_name run timeout"
        print_error_info
        exit 1
    fi
    ret=$(cat /tmp/ltpret)
    if [[ "-$ret" != "-0" ]] ; then
        exit $ret
    fi
}

run_ltptest() {
    echo "run ltp test"
    LTPTestDir=$MntPoint/ltptest
    LtpLog=/tmp/ltp.log
    mkdir -p $LTPTestDir
    nohup /bin/sh -c " /opt/ltp/runltp  -f fs -d $LTPTestDir > $LtpLog 2>&1; echo $? > /tmp/ltpret " &
    wait_proc_done "runltp" $LtpLog
}

run_osstest() {
    echo "run oss test"
    curl https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py
    python /tmp/get-pip.py
    pip install -r /cfs/osstest/requirements.txt
    python /cfs/osstest/run_test.py $LeaderAddr
}


stop_client() {
    echo -n "stop client "
    umount ${MntPoint} && echo "ok" || { echo "failed"; exit 1; }
}

del_vol() {
    echo -n "del vol "
    res=$(curl -s "http://$LeaderAddr/vol/delete?name=$VolName&authKey=$AuthKey")
    code=$(echo "$res" | jq .code)
    if [[ $code -ne 0 ]] ; then
        echo "failed, exit"
        echo "$res" | jq
        exit 1
    fi
    echo "ok"
}

getLeaderAddr
check_status "MetaNode"
check_status "DataNode"
create_vol ; sleep 2
create_dp ; sleep 3
start_client ; sleep 2
run_ltptest; sleep 2
run_osstest; sleep 2
stop_client;
del_vol
