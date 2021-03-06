./bin/spark-submit ~/git/SparkGibbsLDA/target/scala-2.11/sparkgibbslda_2.11-0.1.jar --alpha 0.3 --beta 0.5 --datafile trndocs.dat -d . -m tri -n 100 -K 5 --savestep 50 --twords 10 --wordmap wmtri

bin/spark-submit ~/git/SparkGibbsLDA/target/scala-2.11/sparkgibbslda_2.11-0.1.jar -a 0.3 -b 0.5 -d hdfs://localhost:9000/input -K 5 -m tri -n 100 -t 10 -wm wm_tri

bin/spark-submit ~/git/SparkGibbsLDA/target/scala-2.11/sparkgibbslda_2.11-0.1.jar -a 0.3 -b 0.5 -d hdfs://localhost:9000/input -K 5 -m tri -n 100 -t 10 -wm wm_tri >> ~/git/SparkGibbsLDA/logs.txt

bin/spark-submit ~/git/SparkGibbsLDA/target/scala-2.11/sparkgibbslda_2.11-0.1.jar -a 0.3 -b 0.5 -d hdfs://localhost:9000/input -o ~/git/SparkGibbsLDA/output -K 5 -m tri -n 500 -t 10 -wm wm_tri >> ~/git/SparkGibbsLDA/logs.txt

bin/spark-submit ~/git/SparkGibbsLDA/target/scala-2.11/sparkgibbslda_2.11-0.1.jar -a 0.3 -b 0.5 -d hdfs://localhost:9000/input2 -K 5 -n 100 -t 10  >> ~/git/SparkGibbsLDA/logs.txt

bin/spark-submit --verbose --driver-memory 2560M --executor-memory 7680M ~/git/SparkGibbsLDA/target/scala-2.11/sparkgibbslda_2.11-0.1.jar -a 1.3 -b 1.5 -d hdfs://localhost:9000/input -K 10 -n 500 -t 15  >> ~/git/SparkGibbsLDA/logs2.txt

bin/spark-submit --conf spark.executor.extraJavaOptions=-Xss50m --conf spark.driver.extraJavaOptions=-Xss50m --verbose --driver-memory 7680M --executor-memory 7680M ~/git/SparkGibbsLDA/target/scala-2.11/sparkgibbslda_2.11-0.1.jar -a 0.5 -b 0.3 -d hdfs://localhost:9000/input -K 5 -n 200 -t 10  >> ~/git/SparkGibbsLDA/logsXss8M.txt


