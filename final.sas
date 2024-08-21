libname path "D:\Users\Nick\Downloads\2024\Purdue\Spring 2024\STAT 506\final project\";
dm 'odsresults; clear';
%let daterange=Nov 3rd; /* 7d */
%let name=Nicholas Schiermeier; /* 7d */
%let categories = danceability__, valence__, energy__, acousticness__, instrumentalness__, liveness__, speechiness__;

title "Spotify Streaming Data"; /* 7a */
title2 "From &daterange 2022 - &daterange 2023"; /* 7b */
footnote "&name's Spotify History"; /* 7c */

/* import the csv files I have into sas tables to be able to use and manipulate them */
/* first dataset which is my 2023 listening history */
proc import datafile="D:\Users\Nick\Downloads\2024\Purdue\Spring 2024\STAT 506\final project\FullStreamingHistory.csv" dbms=csv  /* 1a */
	        out=path.SpotifyFinalProject replace;
run;
/* second dataset used for merging and other parts where my data didn't really fit (found on Kaggle) */
proc import datafile="D:\Users\Nick\Downloads\2024\Purdue\Spring 2024\STAT 506\final project\spotify-2023.csv" dbms=csv
			out=path.Top2023Songs replace;
run;

proc contents data=path.SpotifyFinalProject varnum; /* 1b */
run;


/* Data step to "clean up" and add some variables to my table */
data updatedSpotifyData;
	set path.SpotifyFinalProject; /* 5a */
	drop record endTime msPlayed; /* 5c */
	datePlayed = datepart(endTime);
	monthPlayed = month(datePlayed);
	timePlayed = timepart(endTime);
	minsPlayed = msPlayed / 60000; /* 5f */
	length skipped $ 3; /* 5e */
	if minsPlayed < 0.5 then skipped = "Yes"; /* 5f */
	else skipped = "No";
	WHERE trackName IS NOT MISSING; /* 5b */
	format datePlayed mmddyy.; /* 5d */
	format timePlayed time.;
run;
proc print data=updatedSpotifyData(obs=100); run;

/* proc print with various options enabled */
proc print data=updatedSpotifyData(obs=100); /* 3a, 3c */
	var artistName trackName minsPlayed skipped datePlayed; /* 3b */
	where artistName LIKE "Kendrick%"; /* 3d */
	format minsPlayed 4.2 datePlayed DATE10.; /* 3e */ 
run;


/* display data with title and footnote, then clear them */
proc print data=updatedSpotifyData(obs=3); run; /* 7e */
title; footnote; /* 7f */


/* Create two frequency reports, one with mode and key separately, then a two-way freq report between them */
proc freq data=path.Top2023Songs order=data; /* 8a */
	tables mode key / out = spotifyfreqreport;
run;
	
proc freq data=path.Top2023Songs; /* 8b */
	tables mode*key / nocol nopercent; 
run;


/*
 * All sorts required for later use:
 * One is by artists, one is by months, and one is by number of streams 
 */
proc sort data=updatedSpotifyData out=sortedArtists; /* 15d */ 
	by artistName;
run;
proc print data=sortedArtists; run;
/* Sort the number of streams in descending order */
proc sort data=path.Top2023Songs out=sortedStreams; by descending streams; run;
/* sort data based on month so I can create a narrow table from it with monthly minutes */
proc sort data=updatedSpotifyData out=sortedMonths; by monthPlayed; run;

/* Sumarizing data and accumulating the total number of minutes listened / artist */
data artistTotals;
	set sortedArtists;
	drop trackName datePlayed timePlayed skipped minsPlayed monthPlayed;
	by artistName;
	retain totalMins 0; /* 15a */
	if first.artistName then totalMins=0; /* 15c */
	totalMins+minsPlayed; /* 15b */
	if last.artistName then output; /* 15c */
	label totalMins="Total Mins Listened to Artist"; /* This is just for me */
run;

proc print data=artistTotals(obs=100) label; run;

/*
 * Thinking of also doing: 
 *  -20 (20c, not entirely sure what it does?) PG 2.03
 *  -22, 23 (formats, both seem good) PG 2.04
 *  -24 (simple merge and concatening -- wait i only have 1 table idk if i can do this) PG 2.05
 *  -26 (loops) PG 2.06
 *  -28 (hopefully proc transpose is easy) PG 2.07
*/

/* use various functions to add to the top 2023 songs report */
data functions;
	set path.Top2023Songs;
	streams = round(streams, 1000000); /* 20a */
	highest_metric = largest(1, &categories);/* 20b */
	avg_metrics = mean(&categories); /* 20b */
	total_metrics = sum(&categories); /* 20b */
	lowest_metric = min(&categories); /* 20b */
	num_misses = nmiss(&categories); /* 20c */
	highest_chart_ranking = max(in_apple_charts, in_deezer_charts, in_shazam_charts, in_spotify_Charts); /* 20b */
	num_categories = n(&categories); /* 20c */
	keep artist_s__name track_name streams highest_metric avg_metrics total_metrics lowest_metric num_misses highest_chart_ranking num_categories

run;
proc print data=functions(obs=200); run;



/* 
 * Create a format that categorizes how long a user listened to a song 
 * based on number of minutes ,then apply this format in a proc format statement
 */
data lengthFormat; /* 23a */
	retain FmtName 'songLength'; 
	length Label $10;
	Start=0; End = 1; Label="Short"; output;
	Start=1.01; End=2.5; Label="Medium"; output;
	Start=2.51; End=100; Label="Long"; output;
run;
proc format cntlin=lengthFormat; run; /* 23b */
proc print data=updatedspotifydata(obs=100 rename=(minsPlayed=lengthPlayed)); format lengthPlayed songLength.; run; /* 23c */



/* Find how many years it would take to get a billion streams */
/* At the rate of 105% growth / year (This is just an estimate) */
data loops;
	set sortedStreams(obs=200);
	by descending streams; /* 26b */
	ProjectedStreams = streams;
	NumYears = 0;
	do while (ProjectedStreams < 1000000000); /* 26c */
		ProjectedStreams = ProjectedStreams * 1.05;
		NumYears = NumYears + 1;
		output; /* 26d */
	end;
	keep track_name artist_s__name released_year streams ProjectedStreams NumYears;
run;

proc print data=loops; run;



/* convert the narrow monthly table into a wide one that */
/* displays the number of minutes listened each month */
data widetable;
	set sortedMonths;
	by monthPlayed; /* 27b */
	retain monthlyMins 0;
	do Month=1 to 12; /* 26a */
		if monthPlayed=Month then monthlyMins=monthlyMins+minsPlayed; /* 27a */
	end;
	if last.monthPlayed then output; /* 27c */
	keep monthlyMins monthPlayed; /* 27d */
run;

proc print data=widetable noobs; run; /* 27e */
