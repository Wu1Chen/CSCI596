# COVID-19 Contact Tracing

  In the CSCI 596 final project, Our goal is to find out those people who have been in contact with COVID-19 patients, and to test them in order according to the frequencies they contact with the patients. We decide to use Mapreduce, which is a Parallel programming model for data-intensive applications, to compose a program about COVID-19 Contact Tracing.


# What is MapReduce?

  MapReduce is a programming model or pattern within the Hadoop framework that is used to access big data stored in the Hadoop File System. Unlike the traditional data processing system, MapReduce can implement for processing huge amount of data in short with a parallel, distributed algorithm on a cluster.

![image](https://github.com/Wu1Chen/CSCI596/blob/master/pic01.png)

# Our solution
   For people who have direct contact with infected people, we give priority to testing. For people who have indirect contact with infected people, we will test them in order according to the 'value'.
   
### Input：
A B C D  
B A G  
C A E G  
D A E F G  
E C D G  
F D G  
G B C D E F  

(A-B 0  
B-C 1  
C-D 1  
.......  
A-E 1  
A-G 1  
D-G 1    
.......  
A-E 1)  

### Result：
A-B 0  
...  
A-E 2  
A-F 1  
A-G 3  
...  
