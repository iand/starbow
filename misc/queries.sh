#! /bin/sh

echo "http://127.0.0.1:2525/collation/country?q=select+mean(height),+max(height)+where+country='Spain'"
curl "http://127.0.0.1:2525/collation/country?q=select+mean(height),+max(height)+where+country='Spain'"

echo "http://127.0.0.1:2525/collation/country?q=select+count(*)+where+country='France'"
curl "http://127.0.0.1:2525/collation/country?q=select+count(*)+where+country='France'"

echo "http://127.0.0.1:2525/collation/country?q=select+min(height)+where+country='United+States'"
curl "http://127.0.0.1:2525/collation/country?q=select+min(height)+where+country='United+States'"

echo "http://127.0.0.1:2525/collation/tz?q=select+uniquecount(country)+where+tz='Asia/Chongqing'"
curl "http://127.0.0.1:2525/collation/tz?q=select+uniquecount(country)+where+tz='Asia/Chongqing'"

echo "http://127.0.0.1:2525/collation/country?q=select+uniquecount(iata)+where+country='United+States'"
curl "http://127.0.0.1:2525/collation/country?q=select+uniquecount(iata)+where+country='United+States'"
