import com.twitter.scalding._

class MyParallelJob(args:Args) extends Job(args:Args) {

  val init = System.currentTimeMillis // when did the job start ?
  val parallel = args("parallel").toBoolean // parallelize the mapper ?
  val numberOfTasks = args("tasks").toInt // how many tasks concurrently ?

  val input = (1 to 100).toList
  val pipe = IterableSource(input, 'p1)

  def expensiveTask(x:Int):String = {
    Thread.sleep(1000)
    x + "=>" + x*x + "," + (System.currentTimeMillis - init)
  }

  pipe.map('p1->'temp){
    x:Int =>
      if( parallel) Parallelize.tasks( TaskMaker.make(input, expensiveTask) )(numberOfTasks)(flowDef)(x)
      else expensiveTask(x)
   }.write(Tsv("resultpipe"))
}
