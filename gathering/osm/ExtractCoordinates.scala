import java.io.{FileWriter, BufferedWriter}
import scala.io.Source

/**
 * User: Vasily
 * Date: 16.03.14
 * Time: 12:13
 */


val writer = new BufferedWriter(new FileWriter(args(1)));
for (line <- Source.fromFile(args(0)).getLines()) {
   val coord= line.split(",",3)
   writer.write(coord(0).replace("\"","") + " " + coord(1).replace("\"","") + "\n")
}
writer.flush()
writer.close()

