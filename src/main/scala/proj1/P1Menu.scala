package proj1

object P1Menu {

  class Menu(options: Map[Int, String]) {
    //all menu options map
    val menuOptions = options;

    private def menuBorder(): Unit = {
      println("***************************")
    }

    private def menuItems(ind: Int, menVal: String): Unit = {
      val maxLength = 25;
      var tempStr = "|*| "
      tempStr += ind.toString + " -> " + menVal
      while (tempStr.length < (maxLength - 1)) {
        tempStr += " "
      }
      tempStr += "|*|"
      println(tempStr)
    }


    def printMenu(): Unit = {
      //use this to generate the entire menu
      println("")
      println("***************************")
      println("|*|-Options-------------|*|")
      menuOptions.foreach(m => ({
        menuItems(m._1, m._2)
      }))
      menuBorder()
      println("")
    }

    def userOptions(cho: Int): String = {
      //NEED THIS FOR APP
      return menuOptions(cho);
    }
  }
}
