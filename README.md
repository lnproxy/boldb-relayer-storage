# boldb-relayer-storage

boltdb backend for https://github.com/fiatjaf/relayer

Works with the "basic", "expensive", and "whitelisted" examples here https://github.com/fiatjaf/relayer/tree/master/examples
just replace the storage backend with `BoltBackend`.

Here's some benchmarks agains the `SQLite3Backend`:
```
$ go test -bench QueryEvents
goos: ...
goarch: ...
pkg: github.com/lnproxy/boltdb-relayer-storage
cpu: ...
BenchmarkSQLite3QueryEvents/IDs-8         	                25	  53705298 ns/op
BenchmarkSQLite3QueryEvents/Authors-8     	                28	  44777764 ns/op
BenchmarkSQLite3QueryEvents/Tags-8        	                26	  43390974 ns/op
BenchmarkSQLite3QueryEvents/Kinds-8       	                15	  75601363 ns/op
BenchmarkSQLite3QueryEvents/Authors,Kinds-8         	      38	  43619331 ns/op
BenchmarkSQLite3QueryEvents/Since-8                 	       7	 154081020 ns/op
BenchmarkSQLite3QueryEvents/Authors,Since-8         	      31	  42642253 ns/op
BenchmarkSQLite3QueryEvents/Authors,Kinds,Since-8   	      28	  55499216 ns/op
BenchmarkSQLite3QueryEvents/Tags,Kinds-8            	      25	  48259278 ns/op
BenchmarkSQLite3QueryEvents/Tags,Authors-8          	      28	  36721321 ns/op
BenchmarkBoltQueryEvents/IDs-8                      	    2215	    512090 ns/op
BenchmarkBoltQueryEvents/Authors-8                  	     289	   3958450 ns/op
BenchmarkBoltQueryEvents/Tags-8                     	     314	   3761101 ns/op
BenchmarkBoltQueryEvents/Kinds-8                    	     267	   4513275 ns/op
BenchmarkBoltQueryEvents/Authors,Kinds-8            	     162	   7430627 ns/op
BenchmarkBoltQueryEvents/Since-8                    	     322	   3542206 ns/op
BenchmarkBoltQueryEvents/Authors,Since-8            	     328	   4264908 ns/op
BenchmarkBoltQueryEvents/Authors,Kinds,Since-8      	     172	   7663770 ns/op
BenchmarkBoltQueryEvents/Tags,Kinds-8               	     180	   7094083 ns/op
BenchmarkBoltQueryEvents/Tags,Authors-8             	     759	   1616970 ns/op
PASS
ok  	github.com/lnproxy/boltdb-relayer-storage	344.339s
```

