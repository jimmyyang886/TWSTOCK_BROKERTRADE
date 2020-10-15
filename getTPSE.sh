#!/bin/bash
#curl -o "EMdss004."$vardate"-C.csv" -X GET "https://www.tpex.org.tw/web/emergingstock/historical/daily/EMDaily_dl.php?l=zh-tw&f=EMdss004.20201015-C.csv"
vardate=`date +%Y%m%d`
filename="EMdss004."$vardate"-C.csv"
url='https://www.tpex.org.tw/web/emergingstock/historical/daily/EMDaily_dl.php?l=zh-tw&f='$filename
echo $filename
curl -o /home/spark/TPSE/$filename -X GET $url 

