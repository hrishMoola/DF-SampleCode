# CS686 Project 3

ENJOY YOUR SPRING BREAK!

I WILL BE WORKING ON SOMETHING YOU WILL ALL LOVE (aka your future course projects) BUT DO ENJOY YOUR BREAK OK!?

https://github.com/cs-rocks/cs686-lectures/blob/master/projects/project3-README.md



Hello! This is one of my latest projects in Dataflow. The readme above outlines the main tasks to be done.

The starter code provided had only the comments above the functions, and local tests.

In order to run this code :
1. Clone the repo
2. Build the project on IntelliJ from the java/dataflow directory. (All development activity is over here!)
3. execute "gradle cleanTest test -- continue"
4. This will generate the protofiles if needed, and run all the sample tests.


The code:
1. We have to create a custom proto message for intermediary data representation. That is in "profile.proto" 

2. Multiple tasks outlines in README above. Involves creating and using a custom combineFn for merging different purchaseProfiles.Using CombineFn as there could be many of the same key, and GroupByKey would load all into a single iterable on the worker's memory - and that's not good in BIG DATA / Distributed Systems!

3. Extracting high spenders, addicts and purchase profiles based on bundles(apps) is in ExtractData.java in master/java/dataflow/src/main/java/edu/usfca/dataflow/transforms directory.

4. Enjoy!
