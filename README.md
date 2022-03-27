# PyFlink-G05

# Team Members
| Team Member Name           | WorkSpace Link
| ---------------------------|-------------------------|
| 1. JayaShankar Mangina     |https://github.com/jyshnkr/PyFlink-G05/tree/main/JayaShankar                 |
| 2. Pariveshita Thota       |https://github.com/jyshnkr/PyFlink-G05/tree/main/Pariveshita                 |
| 3. Abhilash Ramavaram      |https://github.com/jyshnkr/PyFlink-G05/tree/main/Abhilash                    |
| 4. Madhu Babu Arla         |https://github.com/jyshnkr/PyFlink-G05/tree/main/Madhu                       |
| 5. Sai Naga Anu Teja Gunda |https://github.com/jyshnkr/PyFlink-G05/tree/main/Anutej                      |   
| 6. Nandini Kandi           |https://github.com/jyshnkr/PyFlink-G05/tree/main/Nandini                     |

![GroupCollage](https://user-images.githubusercontent.com/77635770/160168682-61663fb8-2c84-4ed7-8c64-f7e6310d3474.jpeg)

# Description                         
                         
A Big data project to develop Google Page Rank System using Apache Flink with Python.

## Apache Flink

Apache Flink can be described an open-source, unified stream-processing and batch-processing framework developed by the Apache Software Foundation. 
The core of Apache Flink is a distributed streaming data-flow engine written in Java and Scala.
Flink's pipelined runtime system enables the execution of bulk/batch and stream processing programs.
Flink provides a high-throughput, low-latency streaming engine as well as support for event-time processing and state management.
Flink applications are fault-tolerant in the event of machine failure and support exactly-once semantics. 
Programs can be written in Java, Scala, Python, and SQL and are automatically compiled and optimized  into dataflow programs that are executed in a cluster or cloud environment.


## 1. Page rank

PageRank (PR) is an algorithm used by Google Search to rank websites in their search engine results. PageRank works by counting the number and quality of links to a page to determine a rough estimate of how important the website is. The underlying assumption is that more important websites are likely to receive more links from other websites.

## 2. Algorithm Explanation:

The PageRank algorithm outputs a probability distribution used to represent the likelihood that a person randomly clicking on links will arrive at any particular page. PageRank can be calculated for collections of documents of any size. It is assumed in several research papers that the distribution is evenly divided among all documents in the collection at the beginning of the computational process. The PageRank computations require several passes, called “iterations”, through the collection to adjust approximate PageRank values to more closely reflect the theoretical true value.

## 3. Algorithm Explaination with Example:


PageRank (PR) is an algorithm used by Google Search to rank websites in their search engine is used to find out the importance of a page to estimate how good a website is.

![](https://github.com/AbhiRam0099/PyFlink-G05/blob/main/Abhilash/pagerank-algo-image.PNG)

It is not the only algorithm used by Google to order search engine results.
In this topic I will explain

What is PageRank?

·        Page rank is vote which is given by all other pages on the web about how important a particular page on the web is.
·        A link to a page counts as a vote of support.
·        The number of times a page is refers to by the forward link it adds up to the website value.
·        The number of times it is taken as an input to the previous page it also adds up to the web value.


Simplified algorithm of PageRank:
Equation:
PR(A) = (1-d) + d[PR(Ti)/C(Ti) + …. + PR(Tn)/C(Tn)]
Where:
PR(A) = Page Rank of a page (page A)
PR(Ti) = Page Rank of pages Ti which link to page A
C(Ti) = Number of outbound links on page Ti
d = Damping factor which can be set between 0 and 1.


Let’s say we have three pages A, B and C. Where,
1.     A linked to B and C
2.     B linked to C
3.     C linked to A
Calculate Page Rank:
Final Page Rank of a page is determined after many more iterations. Now what is happening at each iteration?
Note: Keeping
·        Standard damping factor  = 0.85
·        At initial stage assume page rank of all page is equal to 1
Iteration 1:
Page Rank of page A:

PR(A) = (1-d) + d[PR(C)/C(C)]   # As only Page C is linked to page A
           = (1-0.85) + 0.85[1/1] # Number of outbound link of Page C = 1(only to A)
           = 0.15 + 0.85
           =           1

Page Rank of page B:

PR(B) = (1-d) + d[PR(A)/C(A)]    # As only Page A is linked to page C
           = (1-0.85) + 0.85[1/2]      # Number of outbound link of Page A = 2 (B and C)
           = 0.15 + 0.425                # and page rank of A was 1 (calculated from previous
           =           0.575                           # step)

Page Rank of page C:

·        As Page A and page B is linked to page C
·        Number of outbound link of Page A [C(A)] = 2 (ie. Page C and Page B)
·        Number of outbound link of Page B [C(B)] = 1 (ie. Page C)
·        PR(A) = 1  (Result from previous step not initial page rank)
·        PR(B) =  0.575 (Result from previous step)
PR(B) = (1-d) + d[PR(A)/C(A) + PR(B)/C(B)]   
           = (1-0.85) + 0.85[(1/2) + (0.575/1)]         
           = 0.15 + 0.85[0.5 + 0.575]                      
           =           1.06375
This is how page rank is calculated at each iteration. In real world it iteration number can be 100, 1000 or may be more than that to come up with final Page Rank score.


[reference](https://thinkinfi.com/page-rank-algorithm-and-implementation-in-python/)


# Contributor's Space

## Abhilash Ramavaram 

I have added Pagerank explanation in detail with an example 

## Anu Teja Gunda

I have added the group image and reference Links for the content and examples we used in this repository

## JayaShankar Mangina

## Madhu Babu Arla

I learned and researched about page rank and page rank algorithm, And posted the same.

## Pariveshita Thota


## Nandini Kandi

I have initiated the wiki for our repository and created sub-pages namely contributions, work, issues, and also allocated sub-pages for every team member.


# References
1. Apache Flink - https://en.wikipedia.org/wiki/Apache_Flink
2. Page Rank Definition and Algorithm Explaination - https://www.geeksforgeeks.org/page-rank-algorithm-implementation/
3. PageRank Example - https://thinkinfi.com/page-rank-algorithm-and-implementation-in-python/
