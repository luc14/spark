# spark
I worked with Python generators, Python pandas and Apache Spark recently. I’m happy to share some experience I’ve got. The generator idea comes from [David Beazley’s website](http://www.dabeaz.com/generators/).

I’m going to use a web log from [kdnuggets web mining course](http://www.kdnuggets.com:80/web_mining_course/) as an example. And I will explain some differences when using the three approaches.

1. The original data needs to fit into RAM with pandas dataframes but not with generators and Spark. Generators take only one line from a file each time and save the result somewhere after processing the data. Spark dataframes work in a similar way but much more complicated.

2. Generators work in Python; pandas mostly works in C; Spark does most things with several workers in parallel in Scala. In most situations, generators would be the slowest. When dataset is small, pandas works faster than Spark since C is faster than Scala and also it doesn’t need manage many workers. When the dataset is big, having several workers in parallel makes Spark faster than pandas. When the dataset is even bigger, pandas wouldn’t work since data cannot be fit into RAM. Spark would work well, but if there isn’t access to the cloud, generators would be a good choice.

3. Generators and Spark wouldn’t compute the data until a result needs to be generated (which is lazy evaluation), but pandas would compute immediately in each execution step (which is greedy evaluation). Generators compute only when called with a for-loop or next() function outside of a generator. Spark computes when an action is requested.

4. pandas dataframes are mutable objects. Spark dataframes and Generators are recipes (functions) to process the data, so there is nothing to change.
