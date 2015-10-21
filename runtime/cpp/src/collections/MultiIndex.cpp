#include "collections/MultiIndex.hpp"

namespace K3 {

#ifdef BSL_ALLOC
	#ifdef BSEQ
	BloombergLP::bdlma::SequentialAllocator mpool;
	#elif BPOOLSEQ
	BloombergLP::bdlma::SequentialAllocator seqpool;
	BloombergLP::bdlma::MultipoolAllocator mpool(8, seqpool);
	#elif BLOCAL
	BloombergLP::bdlma::LocalSequentialAllocator<lsz> mpool;
	#else
	BloombergLP::bdlma::MultipoolAllocator mpool;
	#endif
#endif

};
