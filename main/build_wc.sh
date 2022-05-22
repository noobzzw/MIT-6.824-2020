go build -gcflags 'all=-N -l' -buildmode=plugin ../mrapps/wc.go
#if [ "$(ls -A .| grep -e "mr-.*")" ];then
#  rm -rf mr-*
#fi