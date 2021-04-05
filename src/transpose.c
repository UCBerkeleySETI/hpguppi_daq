#include <xmmintrin.h>
#include <immintrin.h>
#include <emmintrin.h>
#include <smmintrin.h>

// Intrinsics used:
//
// - Load
//   _mm_load_ps (SSE, xmmintrin.h) -> _mm_load_si128 (SSE2, emmintrin.h)
//   _mm256_castps128_ps256 (AVX, immintrin.h) -> _mm256_castsi128_si256 (AVX, immintrin.h)
//   _mm256_insertf128_ps (AVX, immintrin.h) -> _mm256inserti128_si256 (AVX2, immintrin.h) [also ...f128_si256 AVX?]
//
// - Transpose (unpack, shuffle, blend)
//   _mm256_unpacklo_ps (AVX, immintrin.h) -> _mm256_unpacklo_epi32 (AVX2, immintrin.h)
//   _mm256_unpackhi_ps (AVX, immintrin.h) -> _mm256_unpackhi_epi32 (AVX2, immintrin.h)
//   _mm256_shuffle_ps (AVX, immintrin.h) -> NO INTEGER EQUIVALENT!!!
//   _mm256_blend_ps (AVX, immintrin,h) -> _mm256_blend_epi32 (AVX2, immintrin.h)
//
// - Store
//   _mm256_store_ps (AVX, immintrin.h) -> _mm256_store_si256 (AVX, immintrin.h)
//                                         _mm256_stream_si256 (AVX, immintrin.h)
//
// src points to "upper left" corner of source 8x8 matrix
// dst points to "upper left" corner of destination 8x8 matrix (transposed)
// istride is number of 32 bit words from first word of row N of src to first
// word of row N+1 of src
// ostride is number of 32 bit words from first word of row N of dst to first
// word of row N+1 of dst

void transpose_8x8_f32(float * dst, float *src, int istride, int ostride)
{
  __m256  r0, r1, r2, r3, r4, r5, r6, r7;
  __m256  t0, t1, t2, t3, t4, t5, t6, t7;

#if 1
#define load_128(p) _mm_load_ps(p)
#else
#define load_128(p) _mm_castsi128_ps(_mm_stream_load_si128((__m128i*)p))
#endif

  r0 = _mm256_insertf128_ps(_mm256_castps128_ps256(load_128(&src[0*istride+0])), load_128(&src[4*istride+0]), 1);
  r1 = _mm256_insertf128_ps(_mm256_castps128_ps256(load_128(&src[1*istride+0])), load_128(&src[5*istride+0]), 1);
  r2 = _mm256_insertf128_ps(_mm256_castps128_ps256(load_128(&src[2*istride+0])), load_128(&src[6*istride+0]), 1);
  r3 = _mm256_insertf128_ps(_mm256_castps128_ps256(load_128(&src[3*istride+0])), load_128(&src[7*istride+0]), 1);
  r4 = _mm256_insertf128_ps(_mm256_castps128_ps256(load_128(&src[0*istride+4])), load_128(&src[4*istride+4]), 1);
  r5 = _mm256_insertf128_ps(_mm256_castps128_ps256(load_128(&src[1*istride+4])), load_128(&src[5*istride+4]), 1);
  r6 = _mm256_insertf128_ps(_mm256_castps128_ps256(load_128(&src[2*istride+4])), load_128(&src[6*istride+4]), 1);
  r7 = _mm256_insertf128_ps(_mm256_castps128_ps256(load_128(&src[3*istride+4])), load_128(&src[7*istride+4]), 1);

  t0 = _mm256_unpacklo_ps(r0,r1);
  t1 = _mm256_unpackhi_ps(r0,r1);
  t2 = _mm256_unpacklo_ps(r2,r3);
  t3 = _mm256_unpackhi_ps(r2,r3);
  t4 = _mm256_unpacklo_ps(r4,r5);
  t5 = _mm256_unpackhi_ps(r4,r5);
  t6 = _mm256_unpacklo_ps(r6,r7);
  t7 = _mm256_unpackhi_ps(r6,r7);

  __m256 v;

  //r0 = _mm256_shuffle_ps(t0,t2, 0x44);
  //r1 = _mm256_shuffle_ps(t0,t2, 0xEE);  
  v = _mm256_shuffle_ps(t0,t2, 0x4E);
  r0 = _mm256_blend_ps(t0, v, 0xCC);
  r1 = _mm256_blend_ps(t2, v, 0x33);

  //r2 = _mm256_shuffle_ps(t1,t3, 0x44);
  //r3 = _mm256_shuffle_ps(t1,t3, 0xEE);
  v = _mm256_shuffle_ps(t1,t3, 0x4E);
  r2 = _mm256_blend_ps(t1, v, 0xCC);
  r3 = _mm256_blend_ps(t3, v, 0x33);

  //r4 = _mm256_shuffle_ps(t4,t6, 0x44);
  //r5 = _mm256_shuffle_ps(t4,t6, 0xEE);
  v = _mm256_shuffle_ps(t4,t6, 0x4E);
  r4 = _mm256_blend_ps(t4, v, 0xCC);
  r5 = _mm256_blend_ps(t6, v, 0x33);

  //r6 = _mm256_shuffle_ps(t5,t7, 0x44);
  //r7 = _mm256_shuffle_ps(t5,t7, 0xEE);
  v = _mm256_shuffle_ps(t5,t7, 0x4E);
  r6 = _mm256_blend_ps(t5, v, 0xCC);
  r7 = _mm256_blend_ps(t7, v, 0x33);

#if 1
#define store_256(p,v) _mm256_store_ps(p,v)
#else
#define store_256(p,v) _mm256_stream_ps(p,v)
#endif

  store_256(&dst[0*ostride], r0);
  store_256(&dst[1*ostride], r1);
  store_256(&dst[2*ostride], r2);
  store_256(&dst[3*ostride], r3);
  store_256(&dst[4*ostride], r4);
  store_256(&dst[5*ostride], r5);
  store_256(&dst[6*ostride], r6);
  store_256(&dst[7*ostride], r7);
}
