# Movie Recommendation System Using K-Means Clustering

My bachelor thesis at KTH Royal Institute of Technology, where I implemented a recommendation system for movies using K-means. The program uses a movie database called [`omdb0216`](http://www.omdbapi.com/). Recommendations are given to the user based on a movie the user previously like, i.e. the user needs to query a movie to base the recommendations on. The program gives 5 recommendations of movies that the program thinks are relevant. The user can also query based on movie attributes.  The program uses Apache Spark&trade; to do the distributed computing of the k-means algorithm.

## Files 
- [KmeansClusering.scala](/src/org/BachelorThesis/KmeansClusering.scala) The main file that executes the program. The program sorts out the most relevant movies based on IMDB rating > 5.0, as the original database contains over 1M movies. Next the program converts each movie into a unique vector, based on selected attributes. The then program clusters similar movies together and assigns a centroid to each movie using the distributed k-means clustering algorithm in
Apache Spark&trade;. This program also handles the users query and gives the recommendations.

- [Movie.scala](/src/org/BachelorThesis/Movie.scala) Converts each movie in the database to an object.