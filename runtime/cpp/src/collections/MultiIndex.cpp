#include "collections/MultiIndex.hpp"

namespace K3 {

#ifdef BSL_ALLOC
	#ifdef BSEQ
	thread_local BloombergLP::bdlma::SequentialAllocator* mpool;
	#elif BPOOLSEQ
	thread_local BloombergLP::bdlma::SequentialAllocator* seqpool;
	thread_local BloombergLP::bdlma::MultipoolAllocator* mpool;
	#elif BLOCAL
	thread_local BloombergLP::bdlma::LocalSequentialAllocator<lsz>* mpool;
    #elif BCOUNT
    thread_local BloombergLP::bdlma::CountingAllocator* mpool;
	#else
	thread_local BloombergLP::bdlma::MultipoolAllocator* mpool;
	#endif
#endif

};
