/*
 *  Copyright 2011 Tor-Einar Jarnbjo
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package de.jarnbjo.jsnappy;

/**
 * This class provide utility methods for compressing 
 * data blocks using the Snappy algorithm.
 * 
 * @author Tor-Einar Jarnbjo
 * @since 1.0
 */
public class SnappyCompressor {

	// public static final int DEFAULT_MAX_OFFSET = 1024*1024;

	private SnappyCompressor() {
	}
	
	/**
	 * Equivalent to <code>compress(in, 0, in.length, null)</code>.
	 * @param in data to be compressed
	 * @return compressed data block
	 */
	public static Buffer compress(byte[] in) {
		return compress(in, 0, in.length, null);
	}

	/**
	 * Equivalent to <code>compress(in, 0, in.length, out)</code>.
	 * @param in data to be compressed
	 * @param out Buffer for compressed data block
	 * @return reference to <code>out</code>
	 */
	public static Buffer compress(byte[] in, Buffer out) {
		return compress(in, 0, in.length, out);
	}

	/**
	 * Equivalent to <code>compress(in, offset, length, null)</code>.
	 * @param in data to be compressed
	 * @param offset offset in <code>in<code>, on which encoding is started
	 * @param length number of bytes read from the input block 
	 * @return compressed data block
	 */
	public static Buffer compress(byte[] in, int offset, int length) {
		return compress(in, offset, length, null);
	}

	/**
	 * Equivalent to <code>compress(in.getData(), 0, in.getLength(), null)</code>.
	 * @param in data to be compressed
	 * @return compressed data block
	 */
	public static Buffer compress(Buffer in) {
		return compress(in.getData(), 0, in.getLength(), null);
	}

	/**
	 * Equivalent to <code>compress(in.getData(), 0, in.getLength(), out)</code>.
	 * @param in data to be compressed
	 * @param out buffer for decompressed data block
	 * @return reference to <code>out</code>
	 */
	public static Buffer compress(Buffer in, Buffer out) {
		return compress(in.getData(), 0, in.getLength(), out);
	}

	/**
	 * Compress the data contained in <code>in</code> from <code>offset</code>
	 * and <code>length</code> bytes. If an output buffer is provided, the buffer
	 * is reused for the compressed data. If the buffer is too small, its capacity
	 * is expanded to fit the result. If a <code>null</code> argument is passed,
	 * a new buffer is allocated.
	 * @param in
	 * @param offset
	 * @param length
	 * @param out
	 * @return
	 */
	public static Buffer compress(byte[] in, int offset, int length, Buffer out) {

		if(out == null) {
			out = new Buffer(length * 6 / 5);
		}
		else {
			out.ensureCapacity(length * 6 / 5);
		}

		byte[] target = out.getData();
		int targetIndex = 0;
		int lasthit = offset;

		int l = length;
		while(l>0) {
			if(l>=128) {
				target[targetIndex++] = (byte)(0x80 | (l&0x7f));
			}
			else {
				target[targetIndex++] = (byte)l;
			}
			l >>= 7;
		}

		IntListHashMap ilhm = new IntListHashMap(length / 13);

		for(int i = offset; i+4 < length && i < offset+4; i++) {
			ilhm.put(toInt(in, i), i);
		}

		for(int i = offset+4; i < offset + length; i++) {
			Hit h = search(in, i, length, ilhm);
			if(i+4 < offset + length) {
				ilhm.put(toInt(in, i), i);
			}
			if(h != null) {
				if(lasthit < i) {
					int len = i - lasthit - 1;
					if (len < 60) {
						target[targetIndex++] = (byte)(len<<2);
					}
					else if (len < 0x100) {
						target[targetIndex++] = (byte)(60<<2);
						target[targetIndex++] = (byte)len;
					}
					else if (len < 0x10000) {
						target[targetIndex++] = (byte)(61<<2);
						target[targetIndex++] = (byte)len;
						target[targetIndex++] = (byte)(len>>8);
					}
					else if (len < 0x1000000) {
						target[targetIndex++] = (byte)(62<<2);
						target[targetIndex++] = (byte)len;
						target[targetIndex++] = (byte)(len>>8);
						target[targetIndex++] = (byte)(len>>16);
					}
					else {
						target[targetIndex++] = (byte)(63<<2);
						target[targetIndex++] = (byte)len;
						target[targetIndex++] = (byte)(len>>8);
						target[targetIndex++] = (byte)(len>>16);
						target[targetIndex++] = (byte)(len>>24);
					}
					System.arraycopy(in, lasthit, target, targetIndex, i-lasthit);
					targetIndex += i - lasthit;
					lasthit = i;
				}
				if(h.length <= 11 && h.offset < 2048) {
					target[targetIndex] = 1;
					target[targetIndex] |= ((h.length-4)<<2);
					target[targetIndex++] |= (h.offset>>3)&0xe0;
					target[targetIndex++] = (byte)(h.offset&0xff);
				}
				else if (h.offset < 65536) {
					target[targetIndex] = 2;
					target[targetIndex++] |= ((h.length-1)<<2);
					target[targetIndex++] = (byte)(h.offset);
					target[targetIndex++] = (byte)(h.offset>>8);
				}
				else {
					target[targetIndex] = 3;
					target[targetIndex++] |= ((h.length-1)<<2);
					target[targetIndex++] = (byte)(h.offset);
					target[targetIndex++] = (byte)(h.offset>>8);
					target[targetIndex++] = (byte)(h.offset>>16);
					target[targetIndex++] = (byte)(h.offset>>24);
				}
				for(; i < lasthit; i++) {
					if(i + 4 < in.length) {
						ilhm.put(toInt(in, i), i);
					}
				}
				lasthit = i + h.length;
				while(i<lasthit-1) {
					if(i + 4 < in.length) {
						ilhm.put(toInt(in, i), i);
					}
					i++;
				}
			}
			else {
				if(i+4 < length) {
					ilhm.put(toInt(in, i), i);
				}
			}
		}

		if (lasthit < offset + length) {
			int len = (offset+length) - lasthit - 1;
			if (len < 60) {
				target[targetIndex++] = (byte)(len<<2);
			}
			else if (len < 0x100) {
				target[targetIndex++] = (byte)(60<<2);
				target[targetIndex++] = (byte)len;
			}
			else if (len < 0x10000) {
				target[targetIndex++] = (byte)(61<<2);
				target[targetIndex++] = (byte)len;
				target[targetIndex++] = (byte)(len>>8);
			}
			else if (len < 0x1000000) {
				target[targetIndex++] = (byte)(62<<2);
				target[targetIndex++] = (byte)len;
				target[targetIndex++] = (byte)(len>>8);
				target[targetIndex++] = (byte)(len>>16);
			}
			else {
				target[targetIndex++] = (byte)(63<<2);
				target[targetIndex++] = (byte)len;
				target[targetIndex++] = (byte)(len>>8);
				target[targetIndex++] = (byte)(len>>16);
				target[targetIndex++] = (byte)(len>>24);
			}
			System.arraycopy(in, lasthit, target, targetIndex, length - lasthit);
			targetIndex += length - lasthit;
		}

		out.setLength(targetIndex);
		return out;
	}

	private static Hit search(byte[] source, int index, int length, IntListHashMap map) {

		if(index + 4 >= length) {
			// We won't search for backward references if there are less than
			// four bytes left to encode, since no relevant compression can be
			// achieved and the map used to store possible back references uses
			// a four byte key.
			return null;
		}

		if(index > 0 &&
				source[index] == source[index-1] &&
				source[index] == source[index+1] &&
		        source[index] == source[index+2] &&
		        source[index] == source[index+3]) {

			// at least five consecutive bytes, so we do
			// run-length-encoding of the last four
			// (three bytes are required for the encoding,
			// so less than four bytes cannot be compressed)

			int len = 0;
			for(int i = index; len < 64 && i < length && source[index] == source[i]; i++, len++);
			return new Hit(1, len);
		}

		IntIterator ii = map.getReverse(toInt(source, index));
		if(ii == null) {
			return null;
		}

		Hit res = null;

		while(ii.next()) {
			int offset = index - ii.get();
			int l = 0;
			for(int o = index - offset, io = index; io < length && source[o] == source[io] && o < index && l < 64; o++, io++) {
				l++;
			}
			if(l >= 4) {
				if (res == null) {
					res = new Hit();
					res.offset = offset;
					res.length = l;
				}
				else if(l > res.length) {
					res.offset = offset;
					res.length = l;
				}
			}
		}

		return res;
	}

	private static int toInt(byte[] data, int offset) {
		return
			((data[offset]&0xff)<<24) |
			((data[offset+1]&0xff)<<16) |
			((data[offset+2]&0xff)<<8) |
			(data[offset+3]&0xff);
	}

	private static class Hit {
		int offset, length;
		Hit() {
		}
		Hit(int offset, int length) {
			this.offset = offset;
			this.length = length;
		}
	}

}
