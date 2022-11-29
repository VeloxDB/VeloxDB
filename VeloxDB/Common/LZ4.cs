/*
   LZ4 - Fast LZ compression algorithm
   Copyright (C) 2011-present, Yann Collet.
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
    - LZ4 homepage : http://www.lz4.org
    - LZ4 source repository : https://github.com/lz4/lz4
*/

using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;

namespace Velox.Common;

internal unsafe static class LZ4
{
	const int MEMORY_USAGE = 14;
	const int MINMATCH = 4;
	const int HASH_LOG = (MEMORY_USAGE - 2);
	const int HASHTABLESIZE = (1 << HASH_LOG);
	const int HASH_MASK = (HASHTABLESIZE - 1);
	const int NOTCOMPRESSIBLE_DETECTIONLEVEL = 6;
	const int SKIPSTRENGTH = (NOTCOMPRESSIBLE_DETECTIONLEVEL > 2 ? NOTCOMPRESSIBLE_DETECTIONLEVEL : 2);
	const int COPYLENGTH = 8;
	const int LASTLITERALS = 5;
	const int MFLIMIT = (COPYLENGTH + MINMATCH);
	const int MINLENGTH = (MFLIMIT + 1);
	const int MAXD_LOG = 16;
	const int MAX_DISTANCE = ((1 << MAXD_LOG) - 1);
	const int ML_BITS = 4;
	const uint ML_MASK = ((1U << ML_BITS) - 1);
	const int RUN_BITS = (8 - ML_BITS);
	const uint RUN_MASK = ((1U << RUN_BITS) - 1);
	const int STEPSIZE = 8;

	private static bool isHWSupported = Bmi1.X64.IsSupported;

	public static int CompressBound(int isize) { return ((isize) + ((isize) / 255) + 16); }

	public static int Compress(IntPtr source, IntPtr dest, int isize)
	{
		if (isHWSupported)
			return LZ4_compress_HW((byte*)source, (byte*)dest, isize, CompressBound(isize));
		else
			return LZ4_compress((byte*)source, (byte*)dest, isize, CompressBound(isize));
	}

	public static int Decompress(IntPtr source, IntPtr dest, int isize, int maxOutputSize)
	{
		if (isHWSupported)
			return LZ4_uncompress_unknownOutputSize_HW((byte*)source, (byte*)dest, isize, maxOutputSize);
		else
			return LZ4_uncompress_unknownOutputSize((byte*)source, (byte*)dest, isize, maxOutputSize);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static void LZ4_COPYSTEP(ref byte* s, ref byte* d)
	{
		*(ulong*)(d) = *(ulong*)(s); d += 8; s += 8;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static void LZ4_SECURECOPY(ref byte* s, ref byte* d, byte* e)
	{
		if (d < e) LZ4_WILDCOPY(ref s, ref d, e);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static void LZ4_WRITE_LITTLEENDIAN_16(ref byte* p, ushort v)
	{
		*(ushort*)(p) = v; p += 2;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static void LZ4_READ_LITTLEENDIAN_16(ref byte* d, byte* s, byte* p)
	{
		d = (s) - *(ushort*)(p);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static int LZ4_HASH_VALUE(byte* p)
	{
		int i = *(int*)p;
		return (int)((uint)(i * 2654435761) >> (int)((MINMATCH * 8) - HASH_LOG));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static void LZ4_WILDCOPY(ref byte* s, ref byte* d, byte* e)
	{
		do { LZ4_COPYSTEP(ref s, ref d); } while (d < e);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static void LZ4_BLINDCOPY(ref byte* s, ref byte* d, int l)
	{
		byte* e = (d) + l; LZ4_WILDCOPY(ref s, ref d, e); d = e;
	}

	readonly static int[] DeBruijnBytePos = { 0, 0, 0, 0, 0, 1, 1, 2, 0, 3, 1, 3, 1, 4, 2, 7, 0, 2, 3, 6, 1, 5, 3, 5, 1, 3, 4, 4, 2,
		5, 6, 7, 7, 0, 1, 2, 3, 3, 4, 6, 2, 6, 5, 5, 3, 4, 5, 6, 7, 1, 2, 4, 6, 4, 4, 5, 7, 2, 6, 5, 7, 6, 7, 7 };

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static int LZ4_NbCommonBytes(ulong val)
	{
		return DeBruijnBytePos[((ulong)((val & (ulong)-(long)val) * 0x0218A392CDABBD3F)) >> 58];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static int LZ4_NbCommonBytesHW(ulong val)
	{
		return (int)(Bmi1.X64.TrailingZeroCount(val) >> 3);
	}

	private static int LZ4_compress(byte* source, byte* dest, int isize, int maxOutputSize)
	{
		uint* HashTable = stackalloc uint[HASHTABLESIZE];

		byte* ip = (byte*)source;
		byte* pbase = ip;
		byte* anchor = ip;
		byte* iend = ip + isize;
		byte* mflimit = iend - MFLIMIT;
		byte* matchlimit = (iend - LASTLITERALS);

		byte* op = (byte*)dest;
		byte* oend = op + maxOutputSize;

		int length;
		int skipStrength = SKIPSTRENGTH;
		uint forwardH;


		// Init
		if (isize < MINLENGTH) goto _last_literals;

		// First Byte
		HashTable[LZ4_HASH_VALUE(ip)] = (uint)(ip - pbase);
		ip++; forwardH = (uint)LZ4_HASH_VALUE(ip);

		// Main Loop
		for (; ; )
		{
			int findMatchAttempts = (1 << skipStrength) + 3;
			byte* forwardIp = ip;
			byte* pref;
			byte* token;

			// Find a match
			do
			{
				uint h = forwardH;
				int step = findMatchAttempts++ >> skipStrength;
				ip = forwardIp;
				forwardIp = ip + step;

				if (forwardIp > mflimit) { goto _last_literals; }

				forwardH = (uint)LZ4_HASH_VALUE(forwardIp);
				pref = pbase + HashTable[h];
				HashTable[h] = (uint)(ip - pbase);

			} while ((pref < ip - MAX_DISTANCE) || (*(uint*)(pref) != *(uint*)(ip)));

			// Catch up
			while ((ip > anchor) && (pref > (byte*)source) && (ip[-1] == pref[-1])) { ip--; pref--; }

			// Encode Literal length
			length = (int)(ip - anchor);
			token = op++;
			if (op + length + (2 + 1 + LASTLITERALS) + (length >> 8) > oend) return 0;                // Check output limit

			if (length >= (int)RUN_MASK)
			{
				int len;
				*token = (byte)(RUN_MASK << ML_BITS);
				len = (int)(length - RUN_MASK);
				for (; len > 254; len -= 255) *op++ = 255;
				*op++ = (byte)len;
			}
			else *token = (byte)(length << ML_BITS);

			// Copy Literals
			LZ4_BLINDCOPY(ref anchor, ref op, length);

		_next_match:
			// Encode Offset
			LZ4_WRITE_LITTLEENDIAN_16(ref op, (ushort)(ip - pref));

			// Start Counting
			ip += MINMATCH; pref += MINMATCH;    // MinMatch already verified
			anchor = ip;
			while (ip < matchlimit - (STEPSIZE - 1))
			{
				ulong diff = *(ulong*)(pref) ^ *(ulong*)(ip);
				if (diff == 0) { ip += STEPSIZE; pref += STEPSIZE; continue; }
				ip += LZ4_NbCommonBytes(diff);
				goto _endCount;
			}
			if ((ip < (matchlimit - 3)) && (*(uint*)(pref) == *(uint*)(ip))) { ip += 4; pref += 4; }
			if ((ip < (matchlimit - 1)) && (*(ushort*)(pref) == *(ushort*)(ip))) { ip += 2; pref += 2; }
			if ((ip < matchlimit) && (*pref == *ip)) ip++;
			_endCount:

			// Encode MatchLength
			length = (int)(ip - anchor);
			if (op + (1 + LASTLITERALS) + (length >> 8) > oend) return 0;             // Check output limit
			if (length >= (int)ML_MASK)
			{
				*token += (byte)ML_MASK;
				length -= (byte)ML_MASK;
				for (; length > 509; length -= 510) { *op++ = 255; *op++ = 255; }
				if (length > 254) { length -= 255; *op++ = 255; }
				*op++ = (byte)length;
			}
			else *token += (byte)length;

			// Test end of chunk
			if (ip > mflimit) { anchor = ip; break; }

			// Fill table
			HashTable[LZ4_HASH_VALUE(ip - 2)] = (uint)(ip - 2 - pbase);

			// Test next position
			pref = pbase + HashTable[LZ4_HASH_VALUE(ip)];
			HashTable[LZ4_HASH_VALUE(ip)] = (uint)(ip - pbase);
			if ((pref > ip - (MAX_DISTANCE + 1)) && (*(uint*)(pref) == *(uint*)(ip))) { token = op++; *token = 0; goto _next_match; }

			// Prepare next loop
			anchor = ip++;
			forwardH = (uint)LZ4_HASH_VALUE(ip);
		}

	_last_literals:
		// Encode Last Literals
		{
			int lastRun = (int)(iend - anchor);
			if (((byte*)op - dest) + lastRun + 1 + ((lastRun + 255 - RUN_MASK) / 255) > (uint)maxOutputSize) return 0;
			if (lastRun >= (int)RUN_MASK)
			{
				*op++ = (byte)(RUN_MASK << ML_BITS);
				lastRun -= (int)RUN_MASK;
				for (; lastRun > 254; lastRun -= 255) *op++ = 255;
				*op++ = (byte)lastRun;
			}
			else *op++ = (byte)(lastRun << ML_BITS);
			Utils.CopyMemory(anchor, op,(long)(iend - anchor));
			op += iend - anchor;
		}

		// End
		return (int)(((byte*)op) - dest);
	}

	private static int LZ4_compress_HW(byte* source, byte* dest, int isize, int maxOutputSize)
	{
		uint* HashTable = stackalloc uint[HASHTABLESIZE];

		byte* ip = (byte*)source;
		byte* pbase = ip;
		byte* anchor = ip;
		byte* iend = ip + isize;
		byte* mflimit = iend - MFLIMIT;
		byte* matchlimit = (iend - LASTLITERALS);

		byte* op = (byte*)dest;
		byte* oend = op + maxOutputSize;

		int length;
		int skipStrength = SKIPSTRENGTH;
		uint forwardH;


		// Init
		if (isize < MINLENGTH) goto _last_literals;

		// First Byte
		HashTable[LZ4_HASH_VALUE(ip)] = (uint)(ip - pbase);
		ip++; forwardH = (uint)LZ4_HASH_VALUE(ip);

		// Main Loop
		for (; ; )
		{
			int findMatchAttempts = (1 << skipStrength) + 3;
			byte* forwardIp = ip;
			byte* pref;
			byte* token;

			// Find a match
			do
			{
				uint h = forwardH;
				int step = findMatchAttempts++ >> skipStrength;
				ip = forwardIp;
				forwardIp = ip + step;

				if (forwardIp > mflimit) { goto _last_literals; }

				forwardH = (uint)LZ4_HASH_VALUE(forwardIp);
				pref = pbase + HashTable[h];
				HashTable[h] = (uint)(ip - pbase);

			} while ((pref < ip - MAX_DISTANCE) || (*(uint*)(pref) != *(uint*)(ip)));

			// Catch up
			while ((ip > anchor) && (pref > (byte*)source) && (ip[-1] == pref[-1])) { ip--; pref--; }

			// Encode Literal length
			length = (int)(ip - anchor);
			token = op++;
			if (op + length + (2 + 1 + LASTLITERALS) + (length >> 8) > oend) return 0;                // Check output limit

			if (length >= (int)RUN_MASK)
			{
				int len;
				*token = (byte)(RUN_MASK << ML_BITS);
				len = (int)(length - RUN_MASK);
				for (; len > 254; len -= 255) *op++ = 255;
				*op++ = (byte)len;
			}
			else *token = (byte)(length << ML_BITS);

			// Copy Literals
			LZ4_BLINDCOPY(ref anchor, ref op, length);

		_next_match:
			// Encode Offset
			LZ4_WRITE_LITTLEENDIAN_16(ref op, (ushort)(ip - pref));

			// Start Counting
			ip += MINMATCH; pref += MINMATCH;    // MinMatch already verified
			anchor = ip;
			while (ip < matchlimit - (STEPSIZE - 1))
			{
				ulong diff = *(ulong*)(pref) ^ *(ulong*)(ip);
				if (diff == 0) { ip += STEPSIZE; pref += STEPSIZE; continue; }
				ip += LZ4_NbCommonBytesHW(diff);
				goto _endCount;
			}
			if ((ip < (matchlimit - 3)) && (*(uint*)(pref) == *(uint*)(ip))) { ip += 4; pref += 4; }
			if ((ip < (matchlimit - 1)) && (*(ushort*)(pref) == *(ushort*)(ip))) { ip += 2; pref += 2; }
			if ((ip < matchlimit) && (*pref == *ip)) ip++;
			_endCount:

			// Encode MatchLength
			length = (int)(ip - anchor);
			if (op + (1 + LASTLITERALS) + (length >> 8) > oend) return 0;             // Check output limit
			if (length >= (int)ML_MASK)
			{
				*token += (byte)ML_MASK;
				length -= (byte)ML_MASK;
				for (; length > 509; length -= 510) { *op++ = 255; *op++ = 255; }
				if (length > 254) { length -= 255; *op++ = 255; }
				*op++ = (byte)length;
			}
			else *token += (byte)length;

			// Test end of chunk
			if (ip > mflimit) { anchor = ip; break; }

			// Fill table
			HashTable[LZ4_HASH_VALUE(ip - 2)] = (uint)(ip - 2 - pbase);

			// Test next position
			pref = pbase + HashTable[LZ4_HASH_VALUE(ip)];
			HashTable[LZ4_HASH_VALUE(ip)] = (uint)(ip - pbase);
			if ((pref > ip - (MAX_DISTANCE + 1)) && (*(uint*)(pref) == *(uint*)(ip))) { token = op++; *token = 0; goto _next_match; }

			// Prepare next loop
			anchor = ip++;
			forwardH = (uint)LZ4_HASH_VALUE(ip);
		}

	_last_literals:
		// Encode Last Literals
		{
			int lastRun = (int)(iend - anchor);
			if (((byte*)op - dest) + lastRun + 1 + ((lastRun + 255 - RUN_MASK) / 255) > (uint)maxOutputSize) return 0;
			if (lastRun >= (int)RUN_MASK)
			{
				*op++ = (byte)(RUN_MASK << ML_BITS);
				lastRun -= (int)RUN_MASK;
				for (; lastRun > 254; lastRun -= 255) *op++ = 255;
				*op++ = (byte)lastRun;
			}
			else *op++ = (byte)(lastRun << ML_BITS);
			Utils.CopyMemory(anchor, op, (long)(iend - anchor));
			op += iend - anchor;
		}

		// End
		return (int)(((byte*)op) - dest);
	}

	readonly static long[] dec32table = { 0, 3, 2, 3, 0, 0, 0, 0 };
	readonly static long[] dec64table = { 0, 0, 0, -1, 0, 1, 2, 3 };

	private static int LZ4_uncompress_unknownOutputSize(byte* source, byte* dest, int isize, int maxOutputSize)
	{
		// Local Variables
		byte* ip = source;
		byte* iend = ip + isize;
		byte* pref = null;

		byte* op = (byte*)dest;
		byte* oend = op + maxOutputSize;
		byte* cpy;

		// Special case
		if (ip == iend) goto _output_error;    // A correctly formed null-compressed LZ4 must have at least one byte (token=0)

		// Main Loop
		while (true)
		{
			uint token;
			ulong length;

			// get runlength
			token = *ip++;
			if ((length = (token >> ML_BITS)) == RUN_MASK)
			{
				int s = 255;
				while ((ip < iend) && (s == 255)) { s = *ip++; length += (uint)s; }
			}

			// copy literals
			cpy = op + length;
			if ((cpy > oend - MFLIMIT) || (ip + length > iend - (2 + 1 + LASTLITERALS)))
			{
				if (cpy > oend) goto _output_error;          // Error : writes beyond output buffer

				// Error : LZ4 format requires to consume all input at this stage (no match within the
				// last 11 bytes, and at least 8 remaining input bytes for another match+literals)
				if (ip + length != iend) goto _output_error;

				Utils.CopyMemory(ip, op, (long)length);
				op += length;
				break;                                       // Necessarily EOF, due to parsing restrictions
			}
			LZ4_WILDCOPY(ref ip, ref op, cpy); ip -= (op - cpy); op = cpy;

			// get offset
			LZ4_READ_LITTLEENDIAN_16(ref pref, cpy, ip); ip += 2;
			if (pref < dest) goto _output_error;   // Error : offset outside of destination buffer

			// get matchlength
			if ((length = (token & ML_MASK)) == ML_MASK)
			{
				while (ip < iend - (LASTLITERALS + 1))    // Error : a minimum input bytes must remain for LASTLITERALS + token
				{
					int s = *ip++;
					length += (uint)s;
					if (s == 255) continue;
					break;
				}
			}

			// copy repeated sequence
			if (op - pref < STEPSIZE)
			{
				long dec64 = dec64table[(int)(op - pref)];
				op[0] = pref[0];
				op[1] = pref[1];
				op[2] = pref[2];
				op[3] = pref[3];
				op += 4; pref += 4; pref -= dec32table[op - pref];
				*(uint*)(op) = *(uint*)(pref);
				op += STEPSIZE - 4; pref -= dec64;
			}
			else { LZ4_COPYSTEP(ref pref, ref op); }
			cpy = op + length - (STEPSIZE - 4);

			if (cpy > oend - (COPYLENGTH + (STEPSIZE - 4)))
			{
				if (cpy > oend - LASTLITERALS) goto _output_error;    // Error : last 5 bytes must be literals
				LZ4_SECURECOPY(ref pref, ref op, (oend - COPYLENGTH));
				while (op < cpy) *op++ = *pref++;
				op = cpy;
				continue;
			}

			LZ4_WILDCOPY(ref pref, ref op, cpy);
			op = cpy;         // correction
		}

		// end of decoding
		return (int)(op - dest);

	// write overflow error detected
	_output_error:
		return (int)(-(ip - source));
	}

	private static int LZ4_uncompress_unknownOutputSize_HW(byte* source, byte* dest, int isize, int maxOutputSize)
	{
		// Local Variables
		byte* ip = source;
		byte* iend = ip + isize;
		byte* pref = null;

		byte* op = (byte*)dest;
		byte* oend = op + maxOutputSize;
		byte* cpy;

		// Special case
		if (ip == iend) goto _output_error;    // A correctly formed null-compressed LZ4 must have at least one byte (token=0)

		// Main Loop
		while (true)
		{
			uint token;
			ulong length;

			// get runlength
			token = *ip++;
			if ((length = (token >> ML_BITS)) == RUN_MASK)
			{
				int s = 255;
				while ((ip < iend) && (s == 255)) { s = *ip++; length += (uint)s; }
			}

			// copy literals
			cpy = op + length;
			if ((cpy > oend - MFLIMIT) || (ip + length > iend - (2 + 1 + LASTLITERALS)))
			{
				if (cpy > oend) goto _output_error;          // Error : writes beyond output buffer

				// Error : LZ4 format requires to consume all input at this stage (no match within
				// the last 11 bytes, and at least 8 remaining input bytes for another match+literals)
				if (ip + length != iend) goto _output_error;

				Utils.CopyMemory(ip, op, (long)length);
				op += length;
				break;                                       // Necessarily EOF, due to parsing restrictions
			}
			LZ4_WILDCOPY(ref ip, ref op, cpy); ip -= (op - cpy); op = cpy;

			// get offset
			LZ4_READ_LITTLEENDIAN_16(ref pref, cpy, ip); ip += 2;
			if (pref < dest) goto _output_error;   // Error : offset outside of destination buffer

			// get matchlength
			if ((length = (token & ML_MASK)) == ML_MASK)
			{
				while (ip < iend - (LASTLITERALS + 1))    // Error : a minimum input bytes must remain for LASTLITERALS + token
				{
					int s = *ip++;
					length += (uint)s;
					if (s == 255) continue;
					break;
				}
			}

			// copy repeated sequence
			if (op - pref < STEPSIZE)
			{
				long dec64 = dec64table[(int)(op - pref)];
				op[0] = pref[0];
				op[1] = pref[1];
				op[2] = pref[2];
				op[3] = pref[3];
				op += 4; pref += 4; pref -= dec32table[op - pref];
				*(uint*)(op) = *(uint*)(pref);
				op += STEPSIZE - 4; pref -= dec64;
			}
			else { LZ4_COPYSTEP(ref pref, ref op); }
			cpy = op + length - (STEPSIZE - 4);

			if (cpy > oend - (COPYLENGTH + (STEPSIZE - 4)))
			{
				if (cpy > oend - LASTLITERALS) goto _output_error;    // Error : last 5 bytes must be literals
				LZ4_SECURECOPY(ref pref, ref op, (oend - COPYLENGTH));
				while (op < cpy) *op++ = *pref++;
				op = cpy;
				continue;
			}

			LZ4_WILDCOPY(ref pref, ref op, cpy);
			op = cpy;         // correction
		}

		// end of decoding
		return (int)(op - dest);

	// write overflow error detected
	_output_error:
		return (int)(-(ip - source));
	}
}
