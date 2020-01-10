#!/bin/bash
/espace/Auber_PLE-202/spark/bin/spark-submit \
  --class bigdata.Ex7 \
  --master yarn \
  --num-executors 17 \
  --executor-cores 4 \
./target/Ex7*.jar /user/msjuan/cpPhase.csv $