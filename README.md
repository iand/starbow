# Starbow

Starbow is a server for calculating statistics from streaming data.

![starbow](https://github.com/iand/starbow/blob/master/doc/starbow.png)


Starbow aggregates observations from streams and collates statistical data from them according to pre-defined rules, holding them in memory. 
It is not a general purpose database: only the derived statistics from the observation data is stored, the observations are discarded after processing.
Each observation may be routed to zero or more *collations* which are analogous to buckets or tables in a database. 
Collations define a series of statistical measures that are updated for each observation received.
Measures can be precise (e.g. max/min/count/mean) or approximate (e.g. set cardinality). 

The following measures are supported:

* Precise
    - Count 
    - Sum
    - Mean
    - Variance
    - Max
    - Min
 * Approximate
    - Cardinality (implemented using )hyperloglog)

# Collations

Starbow aggregates statistics in "collations". A collation definition
comprises a filter and a set of keys used for routing and grouping
observations and a set of measures used for calculating statistics. Each
unique set of key values encountered results in a new collation instance being
created.

When an observation is received it is routed to the appropriate collation
instance based on the values of the observation's fields corresponding to the
collation's keys. The collation then uses the values of the observation's
measure fields to update its statistics.

Collations can defined via the Starbow API on an ad-hoc basis and participate
in routing as soon as they are created. The API also provides a service for
measuring the memory requirements for a collation given the expected
cardinality of the keys. The memory footprint of a collation will vary heavily
based on the types of measures it employs.

# Status

Starbow is "demoware" and not all features are complete. 
The statistic core of the server works well but the following areas are mostly stubbed out to support demoing and testing:

 * Query Language - supports a very limited SQL dialect of the form `select x,y where z`. This is parsed with regex for simplicity but needs a fuller grammar parser.
 * Query Results - returns human readable dump of results, should define a useful format
 * Server - very basic daemon facilities
 * Collation Creation - currently hardcoded to the demo (see demo.go) but needs to be possible via HTTP API
 * Collation Size Estimation - not supported by API, needed to estimate memory requirements for server
 * Persistence - Starbow is intended to run from memory but it should checkpoint and persist data

In the future the standard measures could be extended by:

* Lookback - limit the measure to a lookback window of time from the present
* Windowed - limit the measure to a a fixed time window
* Bucketed - limit the measure to a series of equally sized time windows

Also several additional approximate measures are planned:

* Containment - allow testing whether specific values have been seen (via bloom filter) 
* Frequency - number of occurrences of a particular value (count-min et al.)
* TopN - most frequent items
* Histogram - counts in various quantiles

# Demo

Compiling and running starbow will automatically start the demo server on port 2525. Two collations are defined for an airport dataset:

 * country
     - counts number of records containing a country
     - height field: supports mean, sum, variance, max and min of airport heights per country
     - iata field: count of unique IATA codes per country
 * tz
     - counts number of records containing a timezone
     - country field: count of unique countries per timezone
     - height field: count of unique heights per timezone

The misc folder contains `airports.sh` that posts the airport data observations to the server and `queries.sh` that posts some sample queries to the server.

## Example Queries

(See Status section above for limitations on the query language implementation)

The country collation contains a precise count of all records that contain a country. This can be queried using `select count(*)`:

    select count(*) where country='United States'

    count(*)=1315

The height field in this collation allows min and max to be queried. These values are precisely calculated from the streaming data received:

    select min(height), max(height) where country='United States'

    min(height)=-54
    max(height)=9078

The iata field is a statistical estimate of the number of unique IATA codes in each country, using hyperloglog. 
This estimate will not be precise. More precise results can be obtained by increasing the precision configured for the field in the collation definition. 
Higher precision estimates require more memory.

    select uniquecount(iata) where country='United States'

    uniquecount(iata)=38


# Observation Format

Observations (stream items) can be sent to the server with an HTTP POST to `/obs` using a custom line based format.

Each line starts with a timestamp in milliseconds. The first byte after the timestamp defines the delimiter for the rest of the line which is assumed to be key/value pairs in the format `key=value`. 
An example:


    1478566442000000|name=Toulouse|country=France|iata=LFBF|height=535|tz=Europe/Paris


# HTTP API

Starbow supports a very simple API:

 * `POST /obs` - send one or more observations to the server
 * `GET+POST /collation/{name}?q={query}` - query a collation
