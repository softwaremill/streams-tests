# streams-tests

To run locally, you'll need some manual setup:

- Change [this file path to your local repo](https://github.com/softwaremill/streams-tests/blob/master/src/main/scala/com/softwaremill/streams/util/TestFiles.scala#L6)
- From the repo root `mkdir files`
- Use eg `sbt run` to run GenerateFiles 3 times, with the [size field](https://github.com/softwaremill/streams-tests/blob/master/src/main/scala/com/softwaremill/streams/util/GenerateFile.scala#L8) having values 10, 100 and 500.  
