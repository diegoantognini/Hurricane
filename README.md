# Hurricane

Many big data applications process large volumes of data using a pipeline of operators, a number of which do not require any ordering of data records. We present Hurricane, a scalable decentralized system that aggregates secondary storage devices in a cluster with the aim of supporting parallel scans of data stored across them. Hurricane spreads input and output data uniformly at random and leverages the absence of order between data blocks to seamlessly balance load and mitigate the effect of stragglers. We implemented a prototype of Hurricane with an HDFS-like RPC interface to facilitate interoperability and show that the resulting system is scalable and seamlessly achieves I/O balance at near-maximal bandwidth.

--

Hurricane is a scalable distributed storage system written in C++, supporting pipelining of order-oblivious operators and doesn’t need any centralized control.
Those operators have an interesting property that their execution on a list of records is independent of the order where the records have been treated which implies that the result will be always correct. Users can access through Hurricane with a large class of programming languages.
