./bin/spark-submit ~/git/SparkGibbsLDA/target/scala-2.11/sparkgibbslda_2.11-0.1.jar --alpha 0.3 --beta 0.5 --datafile trndocs.dat -d . -m tri -n 100 -K 5 --savestep 50 --twords 10 --wordmap wmtri

bin/spark-submit ~/git/SparkGibbsLDA/target/scala-2.11/sparkgibbslda_2.11-0.1.jar -a 0.3 -b 0.5 -d hdfs://localhost:9000/input -K 5 -m tri -n 100 -t 10 -wm wm_tri

bin/spark-submit ~/git/SparkGibbsLDA/target/scala-2.11/sparkgibbslda_2.11-0.1.jar -a 0.3 -b 0.5 -d hdfs://localhost:9000/input -K 5 -m tri -n 100 -t 10 -wm wm_tri >> ~/git/SparkGibbsLDA/logs.txt

