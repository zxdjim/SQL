#!/usr/bin

xdumpling="/usr/bin/dumpling"

#连接信息
user=root
passwd="xxxx"
ip=10.105.220.120
port=4000
db_conn=" -h ${ip} -P ${port} -u ${user} -p ${passwd} "

#备份目录
backup_dir=/data/tidb_bak              # 备份的主目录
backup_txt=$backup_dir/backup.txt      #记录备份开始时间和结束时间

if [ ! -x $xdumpling ]; then
  error "$xdumpling未安装或未链接到/usr/bin."
fi
if [ ! -d $backup_dir ]; then
  mkdir -p ${backup_dir}
fi

echo "备份开始时间:"`date +%Y-%m-%d_%H:%M:%S`>>$backup_txt
${xdumpling}  ${db_conn} -t 8  -r 200000 -F 256MiB -o /data/tidb_bak/`date +%Y%m%d_%H%M%S` >/dev/null 2>&1
echo "备份结束时间:"`date +%Y-%m-%d_%H:%M:%S`>>$backup_txt

#删除过期的全量备份
cd $backup_dir
/usr/bin/find $backup_dir -mindepth 1  -maxdepth 1 -type d -mtime +2 -exec rm -rf {} \;
