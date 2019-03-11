# Clean apha

Some apha data came with sample names that don't match what we have in cassandra
 
Apha sample names have leading zeroes in the sample sequence portion of the sample name 
 
To fix this we have to add sample names that are in cassandra and also try sample names with the leading zeroes removed so e.g. for sample name AF-12-00123-12 we would check if any of the samples like following are in cassandra, and add all the ones that are, duplicating original recording. 
 
    AF-12-00123-12
    AF-12-0123-12
    AF-12-123-12

Hidden in this is the assumption that these samples are all the same, i.e. that leading zeroes do not matter.