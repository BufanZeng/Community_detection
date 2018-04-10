
import java.io._
import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

object Community {
    def CommunitiesAfterCut(vertices:Set[Int], cutmap:Map[Set[Int], Double]): Set[Set[Int]] ={
        var communities = Set.empty[Set[Int]]
        var samecom = Set.empty[Int]
        for (i <- vertices){
            breakable {
                if (samecom.contains(i)) {
                    break()
                }
                else {
                    var oldset = Set(i)
                    var indicator = 0
                    var con = cutmap.keys.filter(_.contains(i)).flatten.toSet.diff(oldset)
                    oldset ++= con
                    while (indicator != 1) {
                        var nextlv = Set.empty[Int]
                        for (j<-con) {
                            nextlv ++= cutmap.keys.filter(_.contains(j)).flatten.toSet.diff(oldset)
                        }
                        if (nextlv.size == 0){
                            indicator = 1
                        }
                        else{
                            con = nextlv
                            oldset ++= nextlv
                        }
                    }
                    communities += oldset
                    samecom ++= vertices.intersect(oldset)
                }
            }
        }
        communities
    }

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Betweenness").setMaster("local[2]")
        var sc = new SparkContext(conf)
        var raw = sc.textFile(args(0))
        var header = raw.first()
        raw = raw.filter(row => row != header)

        val u_p_list = raw.map(row=>{
            var x = row.split(",")
            (x(0),x(1))
        }).groupByKey().map(row=>{
            (row._1.toInt, row._2.toSet)
        }).collect()
        var edgemap = Map.empty[Set[Int], Double]           // 498 edges
        val nrows = u_p_list.length
        for (i<- 0 to nrows -1){
            for (j<-i+1 to nrows -1){
                var mutual = u_p_list(i)._2.intersect(u_p_list(j)._2)
                if (mutual.size >= 7){
                    edgemap += Set(u_p_list(i)._1, u_p_list(j)._1) -> 0
                }
            }
        }
        var vertices = edgemap.flatMap(_._1.toList).toSet   // 222 nodes
        var v_edge_map = Map.empty[Int, Set[Int]]
        for (i<-vertices) {
            var shortpath = Map(i -> 1) // vertice -> int, stores shortest path
            var edge = Map.empty[Int, List[(Int, Int)]] // level -> List(beg, end), stores edge and it's level

            var level = 0
            var indicater = 0
            // setup first level
            var oldset = edgemap.keys.filter(_.contains(i)).toSet
            v_edge_map += (i -> oldset.flatten.diff(Set(i)))
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
            while (indicater != 1) {
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
                    indicater = 1
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
            //            edge(3).filter(_._2 == 1761).map(row=>{(row, shortpath(row._1))}).foreach(println)
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
        // now we have betweenness calculated for each edge in edgemap
        // firstly we rank the betweenness
        val ranks = edgemap.map(_._2).toList.distinct.sorted.reverse        // 271 distinct values
        // cut from top to bottom, store in a Map(modularity -> community)
        val m = edgemap.size
        var cut = 0
        var modmap = Map.empty[(Int, Double), Set[Set[Int]]]
        for (i <- ranks) {
//            println(cut)
            var mod = 0.0
            // find edges with betweenness i in edge map
            var tocut = edgemap.filter(_._2 >= i).keys
            for (n <- tocut){
                var tmp = n.toList
                v_edge_map += (tmp(0) -> v_edge_map(tmp(0)).diff(Set(tmp(1))))
                v_edge_map += (tmp(1) -> v_edge_map(tmp(1)).diff(Set(tmp(0))))
            }
            // remove these edges from edgemap
            var tmpmap = edgemap
            tmpmap --= tocut
            // BFS again on the cut nodes to detect separated communities
            var communities = CommunitiesAfterCut(vertices, tmpmap)
            // for each community calculate the modularity and then sum them up
            for (j <- communities) {
                val tmp = j.toList
                    for (k <- 0 to (tmp.length - 1)){
                        var ki = v_edge_map(tmp(k)).size
                        for (l <- (k+1 to (tmp.length - 1))){
                            var Aij = 0.0
                            if (v_edge_map(tmp(k)).contains(tmp(l))) {
                                Aij = 1.0
                            }
                            var kj = v_edge_map(tmp(l)).size
                            mod += Aij - 0.5 * ki * kj / m
                        }
                    }

            }
            cut += 1
            modmap += ((cut, (0.5 * mod / m)) -> communities)

        }

        // get the highest modularity and sort the corresponding communities
        val out = modmap(modmap.keys.maxBy(_._2)).toList.map(row=>{row.toList.sorted}).sortBy(row => {row(0)})
        val writer = new PrintWriter(new File(args(1)))
        out.map(row=>{
            writer.write("["+ row.mkString(",") +"]\n")
        })
        writer.close()
    }
}