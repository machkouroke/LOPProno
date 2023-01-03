sbt package &&
  sudo cp script/launch-hadoop-job.sh target/scala-2.12/loppronostic_2.12-0.1.1-SNAPSHOT.jar /home/hdoop/Desktop &&
  docker exec hadoop-master /bin/bash -c "cd /home/hdoop/Desktop/Desktop && ./launch-hadoop-job.sh "
