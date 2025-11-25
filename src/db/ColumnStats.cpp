#include <db/ColumnStats.hpp>
#include <numeric>

using namespace db;

ColumnStats::ColumnStats(unsigned buckets, int min, int max)
  : buckets(buckets),
    min(min),
    max(max),
    // bucket_width((max-min+1)/buckets)
    bucket_width((max-min+1)/buckets + ((max-min+1) % buckets != 0))
{
  bucket_height = std::vector<size_t>(buckets);
}

void ColumnStats::addValue(int v) {
  ++bucket_height[(v-min)/bucket_width];
}

size_t countSum(int begin, int end, const std::vector<size_t> &histogram) {
  size_t sum = 0;
  for (int i = begin; i < end; i++){
    sum+=histogram[i];
  }
  return sum;
}

size_t ColumnStats::estimateCardinality(PredicateOp op, int v) const {
  // TODO pa4: some code goes here
  const int v_index = v < min ? 0 : v > max ? buckets : (v-min)/bucket_width;
  const size_t v_cardinality = (v < min || v > max) ? 0 : bucket_height[v_index];
  switch(op){
    case PredicateOp::EQ:
      return v_cardinality/bucket_width;
    case PredicateOp::NE:
      return countSum(0, buckets, bucket_height) - v_cardinality/bucket_width;
    case PredicateOp::LT:{
      int ratio = (((v-min)%bucket_width)*v_cardinality)/bucket_width;
      return ratio + countSum(0, v_index, bucket_height);
    }
    case PredicateOp::LE:{
      int ratio = (((v-min)%bucket_width+1)*v_cardinality)/bucket_width;
      return ratio + countSum(0, v_index, bucket_height);
    }
    case PredicateOp::GT:{
      int ratio = ((bucket_width-(v-min)%bucket_width - 1)*v_cardinality)/bucket_width;
      return ratio + countSum(v_index + 1, buckets, bucket_height);
    }
    case PredicateOp::GE:{
      if (v > max) return 0;
      int ratio = ((bucket_width-(v-min)%bucket_width)*v_cardinality)/bucket_width;
      return ratio + countSum(v_index + 1, buckets, bucket_height);
    }
    default:
      return 0;
  }
}
