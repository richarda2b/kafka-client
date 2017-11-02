val indices = (str: String) => (subString: String) =>
  str.sliding(subString.size)
    .zipWithIndex
    .filter(_._1 == subString)
    .map(_._2)

val find = (haystack: String, needle: String) =>
  needle.permutations.toList.flatMap(indices(haystack)(_))


find("bbbababaaabbbb", "ab").sorted
find("abdcghbaabcdij", "bcda").sorted