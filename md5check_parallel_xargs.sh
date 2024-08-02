#! /bin/bash
thefile=$1
cat ${thefile} | xargs -n 1 -P 4 -I {} sh -c 'echo {} | md5sum -c -'
if [ $? -ne 0 ]; then
  echo "!!!!!!!!!!"
  echo "CHECKSUM VALIDATION FAILED FOR ${thefile}"
  echo "!!!!!!!!!!"
  exit 42
fi
