# starbow



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

Collations can defined via the Starbow API on an ad hoc basis and participate
in routing as soon as they are created. The API also provides a service for
measuring the memory requirements for a collation given the expected
cardinality of the keys. The memory footprint of a collation will vary heavily
based on the types of measures it employes.

Once a collation



# Example Use Case




