
# Q1

App ID:
* app-20240523223428-0013 - sem partição - cache fria
```
24/05/23 22:34:27 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).

users:
Partition 0 has 0 rows                                                          
Partition 1 has 1492696 rows
Partition 2 has 0 rows

questions:
Partition 0 has 330793 rows
Partition 1 has 275845 rows
Partition 2 has 262344 rows
Partition 3 has 251043 rows
Partition 4 has 244781 rows
Partition 5 has 242349 rows
Partition 6 has 237188 rows
Partition 7 has 233680 rows
Partition 8 has 228193 rows
Partition 9 has 223617 rows
Partition 10 has 223196 rows
Partition 11 has 210351 rows
Partition 12 has 36620 rows

answers:
Partition 0 has 538456 rows
Partition 1 has 509937 rows
Partition 2 has 483177 rows
Partition 3 has 450246 rows
Partition 4 has 432404 rows
Partition 5 has 424999 rows
Partition 6 has 408476 rows
Partition 7 has 402046 rows
Partition 8 has 390492 rows
Partition 9 has 371776 rows
Partition 10 has 15294 rows

comments:
Partition 0 has 1689529 rows
Partition 1 has 1772041 rows
Partition 2 has 1774031 rows
Partition 3 has 1779834 rows
Partition 4 has 1751463 rows
Partition 5 has 1694529 rows
Partition 6 has 781377 rows
q1: 25.647s
w1: 204.571s
```

*	app-20240523225732-0014 - sem partição

```
24/05/23 22:57:31 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).

users:
Partition 0 has 0 rows                                                          
Partition 1 has 1492696 rows
Partition 2 has 0 rows

questions:
Partition 0 has 330793 rows
Partition 1 has 275845 rows
Partition 2 has 262344 rows
Partition 3 has 251043 rows
Partition 4 has 244781 rows
Partition 5 has 242349 rows
Partition 6 has 237188 rows
Partition 7 has 233680 rows
Partition 8 has 228193 rows
Partition 9 has 223617 rows
Partition 10 has 223196 rows
Partition 11 has 210351 rows
Partition 12 has 36620 rows

answers:
Partition 0 has 538456 rows
Partition 1 has 509937 rows
Partition 2 has 483177 rows
Partition 3 has 450246 rows
Partition 4 has 432404 rows
Partition 5 has 424999 rows
Partition 6 has 408476 rows
Partition 7 has 402046 rows
Partition 8 has 390492 rows
Partition 9 has 371776 rows
Partition 10 has 15294 rows

comments:
Partition 0 has 1689529 rows
Partition 1 has 1772041 rows
Partition 2 has 1774031 rows
Partition 3 has 1779834 rows
Partition 4 has 1751463 rows
Partition 5 has 1694529 rows
Partition 6 has 781377 rows

q1: 25.315s
w1: 196.113s

```

* app-20240524012725-0036 - ficheiros snappy

Os ficheiros são muito maiores e demoram aproximadamente o dobro do tempo para fazer o scan do parquet


```
24/05/24 01:27:24 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
users:                                                                          
Partition 0 has 0 rows
Partition 1 has 1492696 rows
Partition 2 has 0 rows

questions:
Partition 0 has 226763 rows
Partition 1 has 186394 rows
Partition 2 has 173990 rows
Partition 3 has 167640 rows
Partition 4 has 163000 rows
Partition 5 has 161296 rows
Partition 6 has 157677 rows
Partition 7 has 155639 rows
Partition 8 has 151999 rows
Partition 9 has 149240 rows
Partition 10 has 147534 rows
Partition 11 has 144529 rows
Partition 12 has 104030 rows
Partition 13 has 88354 rows
Partition 14 has 81781 rows
Partition 15 has 81053 rows
Partition 16 has 75662 rows
Partition 17 has 89451 rows
Partition 18 has 83403 rows
Partition 19 has 74377 rows
Partition 20 has 79511 rows
Partition 21 has 76194 rows
Partition 22 has 78041 rows
Partition 23 has 102442 rows

answers:
Partition 0 has 353610 rows
Partition 1 has 341588 rows
Partition 2 has 319498 rows
Partition 3 has 298373 rows
Partition 4 has 286242 rows
Partition 5 has 279578 rows
Partition 6 has 270494 rows
Partition 7 has 265683 rows
Partition 8 has 258117 rows
Partition 9 has 253564 rows
Partition 10 has 184846 rows
Partition 11 has 168349 rows
Partition 12 has 145421 rows
Partition 13 has 163679 rows
Partition 14 has 136363 rows
Partition 15 has 151873 rows
Partition 16 has 137982 rows
Partition 17 has 146162 rows
Partition 18 has 132375 rows
Partition 19 has 133506 rows

comments:
Partition 0 has 1070330 rows
Partition 1 has 1133103 rows
Partition 2 has 1133103 rows
Partition 3 has 1152811 rows
Partition 4 has 1135603 rows
Partition 5 has 1097633 rows
Partition 6 has 781377 rows
Partition 7 has 619199 rows
Partition 8 has 640928 rows
Partition 9 has 638938 rows
Partition 10 has 615860 rows
Partition 11 has 596896 rows
Partition 12 has 627023 rows

q1: 38.802s
w1: 207.02s
```

* Partição de 5 com salt 

```

24/05/24 19:55:00 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
users:                                                                          
Partition 0 has 298835 rows
Partition 1 has 298672 rows
Partition 2 has 298624 rows
Partition 3 has 298306 rows
Partition 4 has 298259 rows

questions:
Partition 0 has 601733 rows
Partition 1 has 600147 rows
Partition 2 has 598818 rows
Partition 3 has 598944 rows
Partition 4 has 600358 rows

answers:
Partition 0 has 885841 rows
Partition 1 has 885167 rows
Partition 2 has 884547 rows
Partition 3 has 885755 rows
Partition 4 has 885993 rows

comments:
Partition 0 has 2250154 rows
Partition 1 has 2248413 rows
Partition 2 has 2247593 rows
Partition 3 has 2246654 rows
Partition 4 has 2249990 rows

q1: 18.193s
w1p: 207.266s

```


# Q2

Sem salt
```

24/05/24 20:25:01 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/05/24 20:32:44 WARN DAGScheduler: Broadcasting large task binary with size 5.3 MiB
votesTypes:                                                                     
Partition 0 has 14 rows

votes:
Partition 0 has 0 rows
Partition 1 has 25556412 rows
Partition 2 has 0 rows

answers:
Partition 0 has 538456 rows
Partition 1 has 509937 rows
Partition 2 has 483177 rows
Partition 3 has 450246 rows
Partition 4 has 432404 rows
Partition 5 has 424999 rows
Partition 6 has 408476 rows
Partition 7 has 402046 rows
Partition 8 has 390492 rows
Partition 9 has 371776 rows
Partition 10 has 15294 rows

users:
Partition 0 has 0 rows
Partition 1 has 1492696 rows
Partition 2 has 0 rows

q2: 455.762s
w2: 683.57s

```