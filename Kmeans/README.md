### Overview
Automatic tagging and cross-linking of documents: Clustering algorithms are frequently used to automatically categorize documents by web services which receive a large
number of new content items daily. As human intervention would be very expensive, these
services need an automated way to find related items and to categorize and cross-link the
documents. News-aggregation site feedly is an example service which processes and categorizes tens of thousands of new documents per day.

### Project Description
This project implements the k-means using Map Reduce where a
single step of Map Reduce completes one iteration of the k-means algorithm. So, to run
k-means for i iterations, we will have to run a sequence of i MapReduce jobs.

#### Files
1. data.txt contains the dataset which has 4601 rows and 58 columns. Each row is a
document represented as a 58 dimensional vector of features. Each component in the
vector represents the importance of a word in the document.
2. vocab.txt contains the words used for generating the document vector. (For example
the first word represents the first feature of the document vector. For document 2 (line
2 in data.txt), feature 3 \alkalemia" (line 3 in vocab.txt) has frequency 0.5, so the
third component of the document vector is 0.5.)
3. c1.txt contains k initial cluster centroids. These centroids were chosen by selecting
k = 10 random points from the input data.
4. c2.txt contains initial cluster centroids which are as far apart as possible. (You can
do this by choosing 1st centroid c1 randomly, and then finding the point c2 that is
farthest from c1, then selecting c3 which is farthest from c1 and c2, and so on).
Use Euclidean distance (ie, the L2 norm) as the distance measure. Set number of iterations
to 20 and number of clusters to 10. Use points in c1.txt for initialization.

### Implement Details
#### Job Chaining
We need to run a sequence of Hadoop jobs where the output of one job will be the input for
the next one. There are multiple ways to do this and you are free to use any method you
are comfortable with. One simple way to handle a such a multistage job is to configure the
output path of the first job to be the input path of the second and so on.

#### Cluster Initialization strategies
The output of k-Means algorithm depends on the initial
points chosen. There are many ways of choosing the initial points. We will compare two of
them: random selection, and selecting points as far apart as possible.