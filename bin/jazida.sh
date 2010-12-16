#!/usr/bin/env bash

# if no args specified, show usage
usage="Formas de uso: jazida.sh ( <startNode> | <search> ) "
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

# set the classpath
export CLASSPATH=./*;
export CLASSPATH=$CLASSPATH:../lib/*;

if [ $1 = "startNode" ]; then
  exec -a JazidaDataNode java -classpath $CLASSPATH br.edu.ifpi.jazida.Jazida startNode $2 $3 $4 $5 $6
  exit 1
fi
if [ $1 = "search" ]; then
  exec -a JazidaClient java -classpath $CLASSPATH br.edu.ifpi.jazida.Jazida search $2
  exit 1
fi
echo "Nenhum comando executado."
