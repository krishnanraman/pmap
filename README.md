pmap
====

Run mappers in parallel using a Scala Actors based paralelization framework

To run without parallelization:

scald.rb --jars ads-batch-deploy.jar:parallelmap.jar --local MyParallelJob.scala --parallel false --tasks 10

To run with parallelization, upto 10 tasks running concurrently:

scald.rb --jars ads-batch-deploy.jar:parallelmap.jar --local MyParallelJob.scala --parallel true --tasks 10

Output of non-parallelization == resultpipe == 100.316 seconds

Output of parallelization == parpipe == only 20.318 sec = 5 times faster!

To run even faster, try running the entire 100 tasks concurrently! This is about 9 times faster!
