
ps -ef|grep peer_fabric|grep -v grep

exit

tail -f peer$1.log
