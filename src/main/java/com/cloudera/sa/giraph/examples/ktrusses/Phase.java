package com.cloudera.sa.giraph.examples.ktrusses;

public enum Phase {
	KCORE, DEGREE, QUERY_FOR_CLOSING_EDGES, FIND_TRIANGLES, TRUSSES, COMPONENTISATION_1, COMPONENTISATION_2, OUTPUT;
}
