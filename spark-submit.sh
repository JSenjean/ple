#!/bin/bash
/espace/Auber_PLE-202/spark/bin/spark-submit \
  --class bigdata.Project \
  --master yarn \
  --driver-cores 68 \
./target/Project-0.0.1.jar /user/msjuan/cpPhase.csv