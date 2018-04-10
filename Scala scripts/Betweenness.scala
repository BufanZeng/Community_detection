import java.io._
import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}

object Betweenness {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Betweenness").setMaster("local[2]")
        var sc = new SparkContext(conf)
        var raw = sc.textFile(args(0))
        var header = raw.first()
        raw = raw.filter(row => row != header)
        // user_prod list
        val u_p_list = raw.map(row=>{
            var x = row.split(",")
            (x(0),x(1))
        }).groupByKey().map(row=>{
            (row._1.toInt, row._2.toSet)
        }).collect()
        // map edges to its betweenness
        var edgemap = Map.empty[Set[Int], Double]
        val nrows = u_p_list.length
        // create graph
        for (i<- 0 to nrows -1){
            for (j<-i+1 to nrows -1){
                var mutual = u_p_list(i)._2.intersect(u_p_list(j)._2)
                if (mutual.size >= 7){
                    edgemap += Set(u_p_list(i)._1, u_p_list(j)._1) -> 0
                }
            }
        }
        var vertices = edgemap.flatMap(_._1.toList).toSet
//        println(edgemap.size)       // 498
//        println(vertices.size)      // 222
        // BFS with shortest path computation
        for (i<-vertices) {
            var shortpath = Map(i -> 1) // vertice -> int, stores shortest path
            var edge = Map.empty[Int, List[(Int, Int)]] // level -> List(beg, end), stores edge and it's level
            var level = 0
            var indicator = 0
            // setup first level
            var oldset = edgemap.keys.filter(_.contains(i)).toSet
            var firstlv = oldset.map(row => {
                (i, row.toList.filter(_ != i)(0))
            }).toList // List(i, xxxx)
            var oldsetsize = oldset.size
            edge += (0 -> firstlv)
            for (j <- firstlv) {
                shortpath += (j._2 -> 1)
            }
            // the lowest level
            var lowestlv = 0
            // BFS algorithm
            while (indicator != 1) {
                var nextlv = List.empty[(Int, Int)]
                for (x <- edge(level)) {
                    var newconnections = edgemap.keys.filter(_.contains(x._2)).toSet.diff(oldset)
                    var connected = newconnections.map(row => {
                        (x._2, row.toList.filter(_ != x._2)(0))
                    }) // (x, new connections)
                    // filter out those connections at same level
                    var validcon = connected.filter(row => {
                        !(edge(level).map(_._2).contains(row._2))
                    })
                    // store the valid connections to next level
                    nextlv = validcon.toList ::: nextlv
                    oldset ++= newconnections
                    // store shorest path
                    for (k <- validcon) {
                        if (shortpath.filterKeys(_ == k._2).isEmpty) {
                            shortpath += (k._2 -> shortpath(x._2))
                        }
                        else {
                            shortpath += (k._2 -> (shortpath(k._2) + shortpath(x._2)))
                        }
                    }
                }

                if (nextlv.isEmpty) {
                    lowestlv = level
                    indicator = 1
                }
                else {
                    level += 1
                    edge += (level -> nextlv)
                }
            }
            // now we have BFS tree, calculate the betweenness
            var btvalue = Map.empty[Int, Double] // vertices -> betweenness value, start with 1
            for (j <- shortpath.keys) {
                btvalue += (j -> 1)
            }
            // loop backwards from bottom to top
            for (l <- lowestlv to 0 by -1) {
                // loop each vertices and add its edges' betweenness to the edgemap
                for (m <- edge(l).map(_._2).distinct) {
                    // find edges that contains m
                    var cand = edge(l).filter(_._2 == m) //.map(row=>{(Set(row._1, row._2), shortpath(row._1))})
                    // distribute betweenness value to edges and add betweenness value to higher nodes
                    val spath = shortpath(m)
                    for (n <- cand) {
                        val bt = 1.0 * btvalue(m) * shortpath(n._1) / spath
                        edgemap += (Set(n._1, n._2) -> (edgemap(Set(n._1, n._2)) + bt))
                        btvalue += (n._1 -> (btvalue(n._1) + bt))
                    }

                }
            }
        }
        val writer = new PrintWriter(new File(args(1)))
        edgemap.toList.map(row=>{
            (row._1.toList.min, row._1.toList.max, row._2)
        }).sortBy(x=>(x._1,x._2))
            .map(row=>{writer.write("(" + row._1 + "," + row._2 + "," + row._3/2 + ")\n")})
        writer.close()
    }

}