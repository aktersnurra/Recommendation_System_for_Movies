{\rtf1\ansi\ansicpg1252\cocoartf1504\cocoasubrtf810
{\fonttbl\f0\fswiss\fcharset0 Helvetica;\f1\fswiss\fcharset0 ArialMT;}
{\colortbl;\red255\green255\blue255;\red0\green0\blue0;\red255\green255\blue255;\red82\green0\blue135;
\red26\green26\blue26;}
{\*\expandedcolortbl;;\csgenericrgb\c0\c0\c0;\cssrgb\c100000\c100000\c100000;\cssrgb\c40000\c0\c60000;
\cssrgb\c13333\c13333\c13333;}
\paperw11900\paperh16840\margl1440\margr1440\vieww10800\viewh8400\viewkind0
\pard\tx566\tx1133\tx1700\tx2267\tx2834\tx3401\tx3968\tx4535\tx5102\tx5669\tx6236\tx6803\pardirnatural\partightenfactor0

\f0\fs24 \cf0 Movie Recommendation System Using K-Means Clustering\
\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\
My bachelor thesis at KTH Royal Institute of Technology, where I implemented a recommendation system for movies using K-means. The Program uses a database of movie called \'92\'92omdb0216\'92\'92 [{\field{\*\fldinst{HYPERLINK "http://www.omdbapi.com/"}}{\fldrslt http://www.omdbapi.com/}}]. Recommendations are given to the user based on a movie the user previously like, i.e. the user needs to query a movie to base the recommendations on. The program gives 5 recommendations of movies that the program thinks are relevant. The user can also query based on movie attributes.  The program uses \cf2 \cb3 \expnd0\expndtw0\kerning0
Apache Spark\'99 to do the distributed computing of the k-means algorithm. 
\f1\fs36 \cf5 \

\f0\fs24 \cf0 \cb1 \kerning1\expnd0\expndtw0  \
Files \
\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\'97\
\
* [KmeansClusering.scala] The main file that executes the program. The program sorts out the most relevant movies based on IMDB rating > 5.0, as the original database contains over 1M movies. Next the program converts each movie into a unique vector, based on selected attributes. The then program clusters similar movies together and assigns a centroid to each movie using the distributed k-means clustering algorithm in \cf2 \cb3 \expnd0\expndtw0\kerning0
Apache Spark\'99\cf0 \cb1 \kerning1\expnd0\expndtw0 . This program also handles the users query and gives the recommendations.\
\
* [Movie.scala] Converts each movie in the database to an object.\
}