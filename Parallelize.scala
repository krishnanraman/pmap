import actors._
import actors.Actor._
import collection.mutable.ListBuffer
import cascading.flow.FlowDef

/*
* A TaskMaker makes a list of tasks
*/
object TaskMaker{
  def make[X,Y](x:List[X], op:X=>Y) = {
    x.map( t => {
      val mytask = new Task[X,Y](){
        def input:X = t
        def compute(input:X)(implicit flowDef:FlowDef):Y = op(input)
      }
      mytask
    })
  }
}

/*
*  a task transforms X => Y
*  eg. say input = List[Double], result = Int
*  the compute function specifies the transform: List[Double] => Int
*/
trait Task[X,Y] {
  def input:X
  def compute(input:X)(implicit flowDef:FlowDef):Y
}

/*
* To execute a bunch of tasks concurrently, say N=4 at a time, do Parallelize.tasks(mytasks)(4)
* If mytasks has 100 tasks, then the first 4 would be done, then the next 4 & so on...until the 100th.
* Typically, N << mytasks.size, so as not to hose the cpu :)
* Since tasks(...) is a Map from input x to its output y, you can just index into it with your input
*/

object Parallelize {
  var notDone = true
  var results:Map[Any,Any] = null

  def tasks[X,Y](mytasks:List[Task[X,Y]])(N:Int)(implicit flowDef:FlowDef):Map[X,Y] = {
    if( notDone ) {
      val main = new Parallelize(mytasks)(N)
      notDone = false
      val myresults = main.result(flowDef) // this will block until all tasks are done
      results = mytasks.map( t=> t.input).zip( myresults).toMap
    }
    results.asInstanceOf[Map[X,Y]]
  }
}

class Parallelize[X,Y](tasks:List[Task[X,Y]])(N:Int) {
  val resbuf = new ListBuffer[Y]()
  val mainactor = self
  var n = 0 // want atomic number

  def result(implicit flowDef:FlowDef):List[Y] = {

    var myactor = new Actor() {

        def mkN = { // make N actors execute 1 task per actor
          val children = List.fill[ComputeActor[X,Y]](N)(new ComputeActor[X,Y]())
          tasks
          .slice(n,n+N)
          .zip(children)
          .foreach(c=> {
            c._2.start
            c._2!(self,c._1)
          })
        }

        def act = {
          mkN
          loop {
            react {
              case (caller:Actor, task:Task[X,Y]) => caller!task
              case (result:Y) => {
                resbuf += result
                n += 1
                resbuf.size match {
                  case z if ((z%N == 0) && (z < tasks.size)) => { println("making next " + N ); mkN }
                  case z if (z >= tasks.size ) => { println("done! "); mainactor!true }
                  case _ => println("Task:" + n)
                }
              }
            }
          }
        }

      }
      myactor.start

    // block until all done
    println("waiting...")
    mainactor.receive {
      case (done:Boolean) => println("All Done!")
      myactor = null
    }
    resbuf.toList
  }
}

case class ComputeActor[X,Y](implicit flowDef:FlowDef) extends Actor {
  def act = {
    loop {
      react {
        case (caller:Actor, task:Task[X,Y]) => {
          caller!(task.compute(task.input)(flowDef):Y)
        }
      }
    }
  }
}
