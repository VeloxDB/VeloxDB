/*
xxHash's License follows:
"""
  Copyright (C) 2012-2015, Yann Collet. (https://github.com/Cyan4973/xxHash)
  BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)
  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are
  met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
 
    * Redistributions in binary form must reproduce the above
      copyright notice, this list of conditions and the following disclaimer
      in the documentation and/or other materials provided with the
      distribution.
  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 
  You can contact the author at :
  - xxHash source repository : https://github.com/Cyan4973/xxHash
"""
*/

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Velox.Common;

internal static class HashUtils
{
    public const uint PrimeMultiplier32 = 2654435761;
    public const ulong PrimeMultiplier64 = 11400714819323198549;

    const uint Prime32_1 = 2654435761U;
    const uint Prime32_2 = 2246822519U;
    const uint Prime32_3 = 3266489917U;
    const uint Prime32_4 = 668265263U;
    const uint Prime32_5 = 374761393U;

    const ulong Prime64_1 = 11400714785074694791UL;
    const ulong Prime64_2 = 14029467366897019727UL;
    const ulong Prime64_3 = 1609587929392839161UL;
    const ulong Prime64_4 = 9650029242287828579UL;
    const ulong Prime64_5 = 2870177450012600261UL;

    public static int CalculatePow2Capacity(int size, float loadFactor, out int limitCapacity)
    {
        long c = CalculatePow2Capacity((long)size, loadFactor, out long lc);
        if (c > int.MaxValue)
            c = lc = (uint.MaxValue / 2);

        limitCapacity = (int)lc;
        return (int)c;
    }

    public static long CalculatePow2Capacity(long size, float loadFactor, out long limitCapacity)
    {
        long capacity = (long)Math.Round((double)size / (double)loadFactor);
        capacity = (long)Utils.GetNextPow2((ulong)capacity);
        limitCapacity = (uint)Math.Round(capacity * loadFactor);
        Checker.AssertTrue(limitCapacity >= size);
        return capacity;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong XXHRotl64(ulong x, int r)
    {
        return ((x << r) | (x >> (64 - r)));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint XXHRotl32(uint x, int r)
    {
        return ((x << r) | (x >> (32 - r)));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint GetHash32(uint v, uint seed)
    {
        uint h = seed + Prime32_5;
        h += v * Prime32_3;
        h = XXHRotl32(h, 17) * Prime32_4;
        h ^= h >> 15;
        h *= Prime32_2;
        h ^= h >> 13;
        h *= Prime32_3;
        h ^= h >> 16;
        return h;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong GetHash64(ulong v, ulong seed)
    {
        ulong h = seed + Prime64_5;
        ulong k1 = v * Prime64_2;
        k1 = XXHRotl64(k1, 31);
        k1 *= Prime64_1;
        h ^= k1;
        h = XXHRotl64(h, 27) * Prime64_1 + Prime64_4;
        h ^= h >> 33;
        h *= Prime64_2;
        h ^= h >> 29;
        h *= Prime64_3;
        h ^= h >> 32;
        return h;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong GetHash96(ulong v1, uint v2, ulong seed)
    {
        ulong h = seed + Prime64_5;
        ulong k1 = v1 * Prime64_2;
        k1 = XXHRotl64(k1, 31);
        k1 *= Prime64_1;
        h ^= k1;
        h = XXHRotl64(h, 27) * Prime64_1 + Prime64_4;
        h ^= (ulong)v2 * Prime64_1;
        h = XXHRotl64(h, 23) * Prime64_2 + Prime64_3;
        h ^= h >> 33;
        h *= Prime64_2;
        h ^= h >> 29;
        h *= Prime64_3;
        h ^= h >> 32;
        return h;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong GetHash128(ulong v1, ulong v2, ulong seed)
    {
        ulong h = seed + Prime64_5;
        ulong k1 = v1 * Prime64_2;
        k1 = XXHRotl64(k1, 31);
        k1 *= Prime64_1;
        h ^= k1;
        h = XXHRotl64(h, 27) * Prime64_1 + Prime64_4;

        k1 = v2 * Prime64_2;
        k1 = XXHRotl64(k1, 31);
        k1 *= Prime64_1;
        h ^= k1;
        h = XXHRotl64(h, 27) * Prime64_1 + Prime64_4;

        h ^= h >> 33;
        h *= Prime64_2;
        h ^= h >> 29;
        h *= Prime64_3;
        h ^= h >> 32;
        return h;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong StartHash64(ulong seed)
    {
        return seed + Prime64_5;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong AdvanceHash64(ulong h, ulong v)
    {
        ulong k1 = v * Prime64_2;
        k1 = XXHRotl64(k1, 31);
        k1 *= Prime64_1;
        h ^= k1;
        return XXHRotl64(h, 27) * Prime64_1 + Prime64_4;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong AdvanceHash64(ulong h, uint v)
    {
        h ^= (ulong)v * Prime64_1;
        return XXHRotl64(h, 23) * Prime64_2 + Prime64_3;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong AdvanceHash64(ulong h, ushort v)
    {
        h ^= v * Prime64_5;
        return XXHRotl64(h, 11) * Prime64_1;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong AdvanceHash64(ulong h, byte v)
    {
        h ^= v * Prime64_5;
        return XXHRotl64(h, 11) * Prime64_1;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong FinishHash64(ulong h)
    {
        h ^= h >> 33;
        h *= Prime64_2;
        h ^= h >> 29;
        h *= Prime64_3;
        h ^= h >> 32;
        return h;
    }
}
