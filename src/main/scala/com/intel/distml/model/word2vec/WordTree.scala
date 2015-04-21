package com.intel.distml.model.word2vec

/**
 * Created by He Yunlong on 7/11/14.
 * for 4*10^7 words, it uses at least 16 * 10^7 = 160M bytes
 */

class WordTree(
    val vocabSize: Int,
    val words: Array[WordNode]
  ) extends Serializable {

  def getWord(index: Int): WordNode = words(index)

  def nodeCount(): Int = words.length

  def bufferingDone(): Unit = {}

  override def clone(): WordTree = {
    val ws = new Array[WordNode](words.length)
    for (i <- 0 until words.length)
      ws(i) = words(i).clone()

    new WordTree(vocabSize, ws)
  }
}
