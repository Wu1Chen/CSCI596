

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

# User specific aliases and functions
export JAVA_HOME=/usr/lib/jvm/java
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
export HADOOP_HOME=/home/ec2-user/hadoop/hadoop-3.1.4
export JAVA_HOME=/usr/lib/jvm/java
``
export PYSPARK_PYTHON=python3 # needed to use Python 3

export PATH=${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${PATH}

