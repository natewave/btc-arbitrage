import $ivy.{`com.softwaremill.sttp::circe:1.5.7`, `com.softwaremill.sttp::core:1.5.7`}

import com.softwaremill.sttp._
import com.softwaremill.sttp.circe._

import scala.util.Try

/*
  A solution to priceonomics Bitcoin Arbitrage puzzle.
  Puzzle link: https://priceonomics.com/jobs/puzzle/
  
  Written in Scala as an Ammonite Script. To run, simply call Ammonite
  with the filename as parameter: 
  $ amm arbitrage.sc
  
  ---
  Explanation:

  If we take the example given by priceonomics, there is an arbitrage loop if we follow
  the sequence $ -> € -> BTC -> $

  Using the rates provided, it means: 1 * 0.7779 * 0.01125 * 115.65 = 1.01209651875
  We start with 1 dollar and end up with 1.01209651875

  We can generalize this pattern and look for a sequence where the product of rates is > 1

  `currency1_currency2_rate * currency2_currency3_rate * ... * Cn_C1 > 1`

  If we model our solution as a graph with each node representing a currency, and the edge
  between two nodes as the exchange rate, we are looking for is a route with the following property:

  `rate(currency1, currency2) * rate(currency2, currency3) * ... * rate(currencyn, currency1) > 1`

  We can easily use brute-force to find such a route if one exists, it's an easy cumulative
  product that's greater than 1, however, a simple Google search shows that if we take the negative
  log of each edge, we can turn the problem of finding a cumulative product that’s greater than 1
  into finding a negative sum cycle in our graph, and that there is an algorithm called Bellman Ford
  that allows for working on negative cycles within a directed graph.
.

  `log(rate(currency1, currency2) * rate(currency2, currency3) * ... * rate(currencyn, currency1)) > log(1)`

  `log(rate(c1, c2)) + log(rate(c2, c3)) + ... + log(rate(cn, c1)) > 0`

  `-log(rate(c1, c2)) -log(rate(c2, c3)) - ... - log(rate(cn, c1)) < 0`

  Bellow is an implementation of such an algorithm in Scala.

  I used sttp library from softwaremill as an http-client and circe for decoding the JSON.

  Bellman Ford for a graph with |V| vertices and |E| edges has an average time complexity of O(|V||E|)
  and space complexity of O(|V|)
*/

sealed trait Currency
case object USD extends Currency
case object EUR extends Currency
case object JPY extends Currency
case object BTC extends Currency
object Currency {
  // todo: use Option
  def apply(currency: String): Currency = currency match {
    case "USD" => USD
    case "EUR" => EUR 
    case "JPY" => JPY
    case "BTC" => BTC
  }
}

case class ExchangeRate(from: Currency, to: Currency, value: Double)

case class GraphEdge(exchange: ExchangeRate)
case class GraphVertex(value: Map[Currency, List[GraphEdge]])

type Distance = Double
type Pred = Option[Currency]
case class Relaxation(cost: Map[Currency, (Distance, Pred)])
object Relaxation {
  def apply(graph: DirectedGraph, source: Currency): Relaxation = {
    // init source distances
    val distances: Map[Currency, (Distance, Pred)]  = graph.vertices.map { case (curr, _) =>
      val d: Distance = if (curr ==  source) 0d else Double.PositiveInfinity
      val p: Pred = Option.empty[Currency]
      (curr, (d, p))
    }

    Relaxation(distances)
  }
}

case class ArbitrageCycle(values: List[Currency])

case class DirectedGraph(vertices: Map[Currency, List[GraphEdge]]) {

  def addEdge(edge: GraphEdge): DirectedGraph = {

    val vertexIn = edge.exchange.from
    val vertexOut = edge.exchange.to

    // add vertices to graph
    val withVertices: DirectedGraph = addVertex(vertexIn).addVertex(vertexOut)

    // add edges to vertices
    val verticesWithEdges: Map[Currency, List[GraphEdge]] = withVertices.vertices.updated(vertexIn, edge +: withVertices.vertices(vertexIn))


    DirectedGraph(verticesWithEdges)
  }

  def addVertex(currency: Currency): DirectedGraph = {
    vertices.get(currency) match {
      case None => DirectedGraph(vertices + (currency -> List.empty))
      case _ => this
    }
  }

  def getEdge(from: Currency, to: Currency): Option[GraphEdge] = {
    vertices.get(from).flatMap { curr =>
      curr.find(e => e.exchange.to == to)
    }
  }

  def edges() = vertices.values.flatten

  def edgesOf(curr: Currency): Seq[GraphEdge] = {
    edges().filter(_.exchange.from == curr).toSeq
  }
}

object DirectedGraph {
  def apply(exchangeRate: ExchangeRate) = new DirectedGraph(Map()).addEdge(GraphEdge(exchangeRate))
  def apply(edge: GraphEdge): DirectedGraph = new DirectedGraph(Map()).addEdge(edge)

  def apply(rates: Seq[ExchangeRate]): DirectedGraph = {
    val emptyGraph = new DirectedGraph(Map())
    rates.foldLeft(emptyGraph) { case (acc, exchangeRate) =>
      acc.addEdge(GraphEdge(exchangeRate))
    }
  }

  def hasNegativeCycle(graph: DirectedGraph, startCurrency: Currency = USD): Boolean = {
    val totalVertices = graph.vertices.keys.size
    val edges = graph.vertices.values.flatten.toSeq
    val totalEdges = graph.vertices.values.map(_.size).sum

    // In a graph without negative-weight cycles,
    // all calculated shortest path values will be optimal after n - 1 iterations.
    val relaxed = relax(graph, USD)

    val opportunity = lastRelaxation(relaxed, graph)

    if (opportunity.values.nonEmpty) {
      val opportunities = USD :: (opportunity.values :+ USD)

      // Print opportunities
      println("[+] Found Arbitrage Opportunity:\n")
      opportunities.sliding(2).foldLeft(1d) { case (amount, element) =>
        // todo: handle effects
        (for {
          currFrom <- Try(element(0)).toOption
          currTo   <- Try(element(1)).toOption
          rate     <- graph.edges.find { edge => (edge.exchange.from == currFrom) && edge.exchange.to == currTo}
        } yield {
          val trade = amount * rate.exchange.value
          println(s"Trade $amount $currFrom to $trade $currTo")

          trade
        }).getOrElse(amount)
      }
    } else {
      println("[-] No Arbitrage Opportunity found.")
    }

    opportunity.values.isEmpty
  }

  def relax(graph: DirectedGraph, source: Currency = USD): Relaxation = {
    // init relaxation
    val relaxation = Relaxation(graph, source)
    val allEdges: Seq[GraphEdge] = graph.vertices.values.flatten.toSeq

    // total vertices - 1 iterations
    val iterations = graph.vertices.keys.size - 1

    (1 to iterations).foldLeft(relaxation) { case (rAcc, _) =>
      relaxEdges(rAcc, allEdges)
    }
  }

  def relaxEdges(relaxation: Relaxation, edges: Seq[GraphEdge]): Relaxation = {
    edges.foldLeft(relaxation) { case (acc, edge) =>

      val from = edge.exchange.from
      val to = edge.exchange.to
      val weight = edge.exchange.value

      (for {
        (fromWeight, _) <- relaxation.cost.get(from)
        (toWeight, _) <- relaxation.cost.get(to)
      } yield {
        val newWeight = fromWeight + weight

        if (newWeight < toWeight) {
          relaxation.copy(cost = relaxation.cost + (to -> (newWeight, Some(from))))
        } else {
          acc
        }
      }).getOrElse(acc)
    }
  }

  def lastRelaxation(relaxation: Relaxation, graph: DirectedGraph): ArbitrageCycle = {
    type Arbitrage = (Relaxation, List[Currency])
    val arbitrage = (relaxation, List.empty[Currency])

    val allEdges: Seq[GraphEdge] = graph.vertices.values.flatten.toSeq

    val (_, cycle) = allEdges.foldLeft(arbitrage) { case (acc, edge) =>
      val (relax, arb) = acc

      val from = edge.exchange.from
      val to = edge.exchange.to
      val weight = edge.exchange.value

      (for {
        (fromWeight, _) <- relax.cost.get(from)
        (toWeight, _) <- relax.cost.get(to)
      } yield {
        val newWeight = fromWeight + weight

        if (newWeight < toWeight) {
          (
            relax.copy(cost = relax.cost + (to -> (newWeight, Some(from)))),
            arb :+ to
          )
        } else {
          (relax, arb)
        }
      }).getOrElse((relax, arb))
    }

    ArbitrageCycle(cycle)
  }
}

object Main {

  def run() = {
    val ratesUrl = "https://fx.priceonomics.com/v1/rates/"
    implicit val backend = HttpURLConnectionBackend()

    // Assume that there is an implicit circe encoder in scope
    // for the request Payload, and a decoder for the MyResponse
    val response = sttp.get(uri"$ratesUrl").response(asJson[Map[String, String]]).send()

    response.body match {
      case Right(body) => {
        body match {
          case Right(parsedBody) => {
            val rates: Seq[ExchangeRate] = parsedBody.map { case (key, value) =>
              val curr = key.split("_")
              ExchangeRate(Currency(curr(0)), Currency(curr(1)), value.toDouble)
            }.toSeq

            val graph = DirectedGraph(rates)

            println("Rates:")
            println("------")
            rates.map(rate => println(s"${rate.from} -> ${rate.to}: ${rate.value.toString}"))
            println("------")
            println("\n[!] Trying to find an arbitrage opportunity...")
            DirectedGraph.hasNegativeCycle(graph)
          }
          case Left(e) => {
            println(s"Error parsing body: $e")
          }
        }
      }
      case Left(e) => {
        println(s"Error getting body from url: $e")
      }
    }
  }
}

Main.run()
