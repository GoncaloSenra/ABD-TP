# Q1

## Original

### Explain Analyse

```
Limit  (cost=11759.76..11760.01 rows=100 width=22) (actual time=155.546..155.570 rows=100 loops=1)
  ->  Sort  (cost=11759.76..11789.03 rows=11708 width=22) (actual time=155.544..155.557 rows=100 loops=1)
        Sort Key: (((count(DISTINCT questions.id) + count(DISTINCT answers.id)) + count(DISTINCT comments.id))) DESC
        Sort Method: top-N heapsort  Memory: 35kB
        ->  GroupAggregate  (cost=10990.32..11312.29 rows=11708 width=22) (actual time=115.509..152.327 rows=11708 loops=1)
              Group Key: u.id
              ->  Sort  (cost=10990.32..11019.59 rows=11708 width=26) (actual time=115.467..117.817 rows=15856 loops=1)
"                    Sort Key: u.id, questions.id"
                    Sort Method: quicksort  Memory: 1099kB
                    ->  Hash Right Join  (cost=6345.37..10199.14 rows=11708 width=26) (actual time=80.979..106.355 rows=15856 loops=1)
                          Hash Cond: (questions.owneruserid = u.id)
                          ->  Seq Scan on questions  (cost=0.00..3756.98 rows=7101 width=12) (actual time=0.073..17.152 rows=7384 loops=1)
                                Filter: ((creationdate <= now()) AND (creationdate >= (now() - '6 mons'::interval)))
                                Rows Removed by Filter: 9893
                          ->  Hash  (cost=6199.02..6199.02 rows=11708 width=22) (actual time=80.811..80.816 rows=13592 loops=1)
                                Buckets: 16384  Batches: 1  Memory Usage: 783kB
                                ->  Hash Right Join  (cost=2721.47..6199.02 rows=11708 width=22) (actual time=65.366..76.660 rows=13592 loops=1)
                                      Hash Cond: (answers.owneruserid = u.id)
                                      ->  Seq Scan on answers  (cost=0.00..3405.35 rows=7044 width=12) (actual time=9.763..15.752 rows=7340 loops=1)
                                            Filter: ((creationdate <= now()) AND (creationdate >= (now() - '6 mons'::interval)))
                                            Rows Removed by Filter: 14622
                                      ->  Hash  (cost=2575.12..2575.12 rows=11708 width=18) (actual time=55.421..55.424 rows=11770 loops=1)
                                            Buckets: 16384  Batches: 1  Memory Usage: 662kB
                                            ->  Hash Right Join  (cost=564.43..2575.12 rows=11708 width=18) (actual time=34.733..51.291 rows=11770 loops=1)
                                                  Hash Cond: (comments.userid = u.id)
                                                  ->  Seq Scan on comments  (cost=0.00..2009.85 rows=322 width=12) (actual time=22.071..35.225 rows=321 loops=1)
                                                        Filter: ((creationdate <= now()) AND (creationdate >= (now() - '6 mons'::interval)))
                                                        Rows Removed by Filter: 37450
                                                  ->  Hash  (cost=418.08..418.08 rows=11708 width=14) (actual time=12.253..12.254 rows=11708 loops=1)
                                                        Buckets: 16384  Batches: 1  Memory Usage: 658kB
                                                        ->  Seq Scan on users u  (cost=0.00..418.08 rows=11708 width=14) (actual time=0.043..6.784 rows=11708 loops=1)
Planning Time: 1.229 ms
Execution Time: 156.148 ms
```

```csv
id,displayname,total
441016,1.21 gigawatts,9
2205377,Yudi,7
2206146,servvs,7
4269535,Erick Ramirez,7
17654126,Noureddin Sameer,7
2042748,Pattle,7
18525211,EtherealCodingPro,7
3358272,r2evans,7
9756894,Diroallu,7
14252075,Woody,7
1114223,DevStarlight,7
22405757,Richard Haley,7
7163504,Oiko,6
22910982,Alvin Xiong,6
410867,Cameron Lowell Palmer,6
6550982,ashmi123,6
6107217,Eric,6
5269818,wabray,6
23176982,Alchemy Creative,6
11232091,moys,6
3733132,user3733132,6
14068996,hamstar,6
4186834,meitme,6
1890745,xiepan,6
1147455,dif,6
192044,Be Kind To New Users,6
674041,Artem,6
22813481,user121212121,6
5057428,Ronit kadwane,6
12555318,Gilush,6
14736508,Krishanu dev,6
202009,Andy Thomas,6
8579633,Dawid Schmidt,6
13746394,rob0tst0p,6
1843011,Remis Haroon - رامز,6
23425102,Daniel Jose Pereira,6
3759703,phzonta,6
1689274,user1689274,6
10847510,john,6
7751821,Shubham Sharma,5
7868565,John Doah,5
6209062,lauren,5
908245,sina,5
6522197,msk,5
8251717,sky wing,5
5730186,YAM,5
166231,Terry,5
5762232,Sigit Purnomo,5
5015569,blacksite,5
4967292,json,5
5042990,Donnie Darko,5
5231877,Ivan Semochkin,5
5308398,Criss,5
783912,FlyingFoX,5
4615231,ProgrammingRookie,5
6045817,欧阳维杰,5
836018,Jack,5
848871,Me hdi,5
149578,Ricky,5
4518341,wjandrea,5
7656386,dbstyles,5
7734308,Kenneth B. Jensen,5
4952130,Dimitris Fasarakis Hilliard,5
5961857,Emrena,5
8437546,ProteinGuy,5
621817,DagonAmigaOS,5
3176336,Manic Depression,5
3970411,Hamid Pourjam,5
2946873,Alan Wells,5
2796952,user2796952,5
2976878,Hamish,5
4032070,lbug,5
543920,Jacob,5
2051427,kKdH,5
2105986,faizal,5
1947134,Heyyo,5
1848641,AndreA,5
481075,MysteryMoose,5
2029983,Thom A,5
2253302,alexander.polomodov,5
487339,DSM,5
102315,Alper,5
1589447,Sudheer Kumar Palchuri,5
355371,Moudy,5
1576344,mel,5
2109064,Michael,5
1751977,Ashbury,5
2668555,Junior Hernando Huaman Flores,5
2826147,Amit Vaghela,5
2869127,Jacob Ian,5
1526396,TinyTiger,5
3220206,Joel Alvarez Myrrie,5
1489885,HAS,5
1548094,ɐsɹǝʌ ǝɔıʌ,5
447040,Gobliins,5
813951,Mister Smith,5
4539709,0m3r,5
4561786,JSNoob,5
741249,THelper,5
328193,David,5

```

## V1

### Explain Analyse

```
Limit  (cost=11647.43..11647.68 rows=100 width=22) (actual time=128.637..128.669 rows=100 loops=1)
  ->  Sort  (cost=11647.43..11676.70 rows=11708 width=22) (actual time=128.634..128.655 rows=100 loops=1)
"        Sort Key: (((COALESCE(q.q_total, '0'::bigint) + COALESCE(a.a_total, '0'::bigint)) + COALESCE(c.c_total, '0'::bigint))) DESC, u.displayname"
        Sort Method: top-N heapsort  Memory: 36kB
        ->  Hash Left Join  (cost=10631.10..11199.95 rows=11708 width=22) (actual time=98.950..121.141 rows=11708 loops=1)
              Hash Cond: (u.id = c.userid)
              ->  Hash Left Join  (cost=8595.02..9074.59 rows=11708 width=30) (actual time=75.022..92.782 rows=11708 loops=1)
                    Hash Cond: (u.id = a.owneruserid)
                    ->  Hash Left Join  (cost=4467.64..4916.47 rows=11708 width=22) (actual time=40.383..52.262 rows=11708 loops=1)
                          Hash Cond: (u.id = q.owneruserid)
                          ->  Seq Scan on users u  (cost=0.00..418.08 rows=11708 width=14) (actual time=0.034..2.107 rows=11708 loops=1)
                          ->  Hash  (cost=4389.50..4389.50 rows=6251 width=16) (actual time=40.288..40.292 rows=5469 loops=1)
                                Buckets: 8192  Batches: 1  Memory Usage: 321kB
                                ->  Subquery Scan on q  (cost=4211.23..4389.50 rows=6251 width=16) (actual time=21.383..38.275 rows=5469 loops=1)
                                      ->  GroupAggregate  (cost=4211.23..4326.99 rows=6251 width=16) (actual time=21.382..37.493 rows=5469 loops=1)
                                            Group Key: questions.owneruserid
                                            ->  Sort  (cost=4211.23..4228.98 rows=7101 width=12) (actual time=21.355..22.970 rows=7384 loops=1)
"                                                  Sort Key: questions.owneruserid, questions.id"
                                                  Sort Method: quicksort  Memory: 481kB
                                                  ->  Seq Scan on questions  (cost=0.00..3756.98 rows=7101 width=12) (actual time=0.077..18.284 rows=7384 loops=1)
                                                        Filter: ((creationdate <= now()) AND (creationdate >= (now() - '6 mons'::interval)))
                                                        Rows Removed by Filter: 9893
                    ->  Hash  (cost=4043.15..4043.15 rows=6739 width=16) (actual time=34.569..34.572 rows=5537 loops=1)
                          Buckets: 8192  Batches: 1  Memory Usage: 324kB
                          ->  Subquery Scan on a  (cost=3855.54..4043.15 rows=6739 width=16) (actual time=27.578..33.133 rows=5537 loops=1)
                                ->  GroupAggregate  (cost=3855.54..3975.76 rows=6739 width=16) (actual time=27.576..32.337 rows=5537 loops=1)
                                      Group Key: answers.owneruserid
                                      ->  Sort  (cost=3855.54..3873.15 rows=7044 width=12) (actual time=27.563..28.456 rows=7340 loops=1)
"                                            Sort Key: answers.owneruserid, answers.id"
                                            Sort Method: quicksort  Memory: 479kB
                                            ->  Seq Scan on answers  (cost=0.00..3405.35 rows=7044 width=12) (actual time=18.322..24.709 rows=7340 loops=1)
                                                  Filter: ((creationdate <= now()) AND (creationdate >= (now() - '6 mons'::interval)))
                                                  Rows Removed by Filter: 14622
              ->  Hash  (cost=2032.08..2032.08 rows=320 width=16) (actual time=23.904..23.908 rows=221 loops=1)
                    Buckets: 1024  Batches: 1  Memory Usage: 19kB
                    ->  Subquery Scan on c  (cost=2023.26..2032.08 rows=320 width=16) (actual time=23.668..23.831 rows=221 loops=1)
                          ->  GroupAggregate  (cost=2023.26..2028.88 rows=320 width=16) (actual time=23.666..23.802 rows=221 loops=1)
                                Group Key: comments.userid
                                ->  Sort  (cost=2023.26..2024.07 rows=322 width=12) (actual time=23.651..23.675 rows=321 loops=1)
"                                      Sort Key: comments.userid, comments.id"
                                      Sort Method: quicksort  Memory: 37kB
                                      ->  Seq Scan on comments  (cost=0.00..2009.85 rows=322 width=12) (actual time=13.991..23.535 rows=321 loops=1)
                                            Filter: ((creationdate <= now()) AND (creationdate >= (now() - '6 mons'::interval)))
                                            Rows Removed by Filter: 37450
Planning Time: 0.957 ms
Execution Time: 83.288 ms


```

```csv

id,displayname,total
441016,1.21 gigawatts,9
2205377,Yudi,7
2206146,servvs,7
4269535,Erick Ramirez,7
17654126,Noureddin Sameer,7
2042748,Pattle,7
18525211,EtherealCodingPro,7
3358272,r2evans,7
9756894,Diroallu,7
14252075,Woody,7
1114223,DevStarlight,7
22405757,Richard Haley,7
7163504,Oiko,6
22910982,Alvin Xiong,6
410867,Cameron Lowell Palmer,6
6550982,ashmi123,6
6107217,Eric,6
5269818,wabray,6
23176982,Alchemy Creative,6
11232091,moys,6
3733132,user3733132,6
14068996,hamstar,6
4186834,meitme,6
1890745,xiepan,6
1147455,dif,6
192044,Be Kind To New Users,6
674041,Artem,6
22813481,user121212121,6
5057428,Ronit kadwane,6
12555318,Gilush,6
14736508,Krishanu dev,6
202009,Andy Thomas,6
8579633,Dawid Schmidt,6
13746394,rob0tst0p,6
1843011,Remis Haroon - رامز,6
23425102,Daniel Jose Pereira,6
3759703,phzonta,6
1689274,user1689274,6
10847510,john,6
7751821,Shubham Sharma,5
7868565,John Doah,5
6209062,lauren,5
908245,sina,5
6522197,msk,5
8251717,sky wing,5
5730186,YAM,5
166231,Terry,5
5762232,Sigit Purnomo,5
5015569,blacksite,5
4967292,json,5
5042990,Donnie Darko,5
5231877,Ivan Semochkin,5
5308398,Criss,5
783912,FlyingFoX,5
4615231,ProgrammingRookie,5
6045817,欧阳维杰,5
836018,Jack,5
848871,Me hdi,5
149578,Ricky,5
4518341,wjandrea,5
7656386,dbstyles,5
7734308,Kenneth B. Jensen,5
4952130,Dimitris Fasarakis Hilliard,5
5961857,Emrena,5
8437546,ProteinGuy,5
621817,DagonAmigaOS,5
3176336,Manic Depression,5
3970411,Hamid Pourjam,5
2946873,Alan Wells,5
2796952,user2796952,5
2976878,Hamish,5
4032070,lbug,5
543920,Jacob,5
2051427,kKdH,5
2105986,faizal,5
1947134,Heyyo,5
1848641,AndreA,5
481075,MysteryMoose,5
2029983,Thom A,5
2253302,alexander.polomodov,5
487339,DSM,5
102315,Alper,5
1589447,Sudheer Kumar Palchuri,5
355371,Moudy,5
1576344,mel,5
2109064,Michael,5
1751977,Ashbury,5
2668555,Junior Hernando Huaman Flores,5
2826147,Amit Vaghela,5
2869127,Jacob Ian,5
1526396,TinyTiger,5
3220206,Joel Alvarez Myrrie,5
1489885,HAS,5
1548094,ɐsɹǝʌ ǝɔıʌ,5
447040,Gobliins,5
813951,Mister Smith,5
4539709,0m3r,5
4561786,JSNoob,5
741249,THelper,5
328193,David,5

```

## V2

### Explain Analyse

```
Limit  (cost=5509.93..5510.18 rows=100 width=22) (actual time=95.457..96.949 rows=100 loops=1)
  ->  Sort  (cost=5509.93..5539.20 rows=11708 width=22) (actual time=95.453..96.933 rows=100 loops=1)
"        Sort Key: (COALESCE(""*SELECT* 3"".total, '0'::bigint)) DESC"
        Sort Method: top-N heapsort  Memory: 34kB
        ->  Hash Right Join  (cost=3378.19..5062.46 rows=11708 width=22) (actual time=25.848..94.530 rows=11769 loops=1)
"              Hash Cond: (""*SELECT* 3"".userid = u.id)"
              ->  Gather  (cost=2813.76..4496.93 rows=418 width=16) (actual time=17.093..81.992 rows=368 loops=1)
                    Workers Planned: 2
                    Workers Launched: 2
                    ->  Parallel Append  (cost=1813.76..3455.13 rows=174 width=16) (actual time=4.937..21.789 rows=123 loops=3)
"                          ->  Subquery Scan on ""*SELECT* 3""  (cost=2019.85..2026.73 rows=250 width=16) (actual time=29.488..29.641 rows=191 loops=1)"
                                ->  GroupAggregate  (cost=2019.85..2024.23 rows=250 width=16) (actual time=29.486..29.613 rows=191 loops=1)
                                      Group Key: comments.userid
                                      ->  Sort  (cost=2019.85..2020.48 rows=251 width=12) (actual time=29.470..29.491 rows=283 loops=1)
"                                            Sort Key: comments.userid, comments.id"
                                            Sort Method: quicksort  Memory: 36kB
                                            ->  Seq Scan on comments  (cost=0.00..2009.85 rows=251 width=12) (actual time=16.376..29.309 rows=283 loops=1)
                                                  Filter: ((creationdate <= now()) AND (creationdate >= (now() - '6 mons'::interval)))
                                                  Rows Removed by Filter: 37488
"                          ->  Subquery Scan on ""*SELECT* 2""  (cost=1813.76..1815.93 rows=79 width=16) (actual time=20.673..20.735 rows=86 loops=1)"
                                ->  GroupAggregate  (cost=1813.76..1815.14 rows=79 width=16) (actual time=20.671..20.721 rows=86 loops=1)
                                      Group Key: answers.owneruserid
                                      ->  Sort  (cost=1813.76..1813.95 rows=79 width=12) (actual time=20.653..20.662 rows=86 loops=1)
"                                            Sort Key: answers.owneruserid, answers.id"
                                            Sort Method: quicksort  Memory: 28kB
                                            ->  Seq Scan on answers  (cost=0.00..1811.27 rows=79 width=12) (actual time=20.361..20.591 rows=86 loops=1)
                                                  Filter: ((creationdate <= now()) AND (creationdate >= (now() - '6 mons'::interval)))
                                                  Rows Removed by Filter: 14637
"                          ->  Subquery Scan on ""*SELECT* 1""  (cost=1635.88..1638.33 rows=89 width=16) (actual time=14.806..14.931 rows=91 loops=1)"
                                ->  GroupAggregate  (cost=1635.88..1637.44 rows=89 width=16) (actual time=14.796..14.905 rows=91 loops=1)
                                      Group Key: questions.owneruserid
                                      ->  Sort  (cost=1635.88..1636.10 rows=89 width=12) (actual time=14.767..14.779 rows=91 loops=1)
"                                            Sort Key: questions.owneruserid, questions.id"
                                            Sort Method: quicksort  Memory: 28kB
                                            ->  Seq Scan on questions  (cost=0.00..1633.00 rows=89 width=12) (actual time=14.254..14.590 rows=91 loops=1)
                                                  Filter: ((creationdate <= now()) AND (creationdate >= (now() - '6 mons'::interval)))
                                                  Rows Removed by Filter: 9909
              ->  Hash  (cost=418.08..418.08 rows=11708 width=14) (actual time=8.644..8.651 rows=11708 loops=1)
                    Buckets: 16384  Batches: 1  Memory Usage: 658kB
                    ->  Seq Scan on users u  (cost=0.00..418.08 rows=11708 width=14) (actual time=0.066..5.675 rows=11708 loops=1)
```

## Comparação

### Sem alterações nas tabelas

Média de tempo de execução:

* Original - 94.29 ms
* V1 - 59.20 ms => ganho de 35.09 ms
* V2 - 85.22 ms => ganho de 9.07 ms

### Index

```postgresql
create index questions_owneruserid_idx on questions (owneruserid);
```

```postgresql
create index answers_owneruserid_idx on answers (owneruserid);
```

```postgresql
create index comments_owneruserid_idx on comments (userid);
```
