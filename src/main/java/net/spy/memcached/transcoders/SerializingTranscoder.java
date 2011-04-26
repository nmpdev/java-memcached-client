// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.transcoders;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;

import net.spy.memcached.CachedData;
import org.hibernate.cache.entry.CacheEntry;
import org.msgpack.MessageTypeException;
import org.msgpack.Packer;
import org.msgpack.Unpacker;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyException;

/**
 * Transcoder that serializes and compresses objects.
 */
public class SerializingTranscoder extends BaseSerializingTranscoder
	implements Transcoder<Object> {
    static boolean WITH_CACHE_ENTRY = true;
    static {
        if( null != System.getProperty("WITHOUT_CACHE_ENTRY") ) {
            WITH_CACHE_ENTRY = false;
        }
    }

	// General flags
	static final int SERIALIZED=1;
	static final int COMPRESSED=2;
    static final int CACHE_ENTRY=4;


	// Special flags for specially handled types.
	private static final int SPECIAL_MASK=0xff00;
	static final int SPECIAL_BOOLEAN=(1<<8);
	static final int SPECIAL_INT=(2<<8);
	static final int SPECIAL_LONG=(3<<8);
	static final int SPECIAL_DATE=(4<<8);
	static final int SPECIAL_BYTE=(5<<8);
	static final int SPECIAL_FLOAT=(6<<8);
	static final int SPECIAL_DOUBLE=(7<<8);
	static final int SPECIAL_BYTEARRAY=(8<<8);

	private final TranscoderUtils tu=new TranscoderUtils(true);

	/**
	 * Get a serializing transcoder with the default max data size.
	 */
	public SerializingTranscoder() {
		this(CachedData.MAX_SIZE);
	}

	/**
	 * Get a serializing transcoder that specifies the max data size.
	 */
	public SerializingTranscoder(int max) {
		super(max);
	}

	@Override
	public boolean asyncDecode(CachedData d) {
		if((d.getFlags() & COMPRESSED) != 0
				|| (d.getFlags() & SERIALIZED) != 0
                || (d.getFlags() & CACHE_ENTRY) != 0) {
			return true;
		}
		return super.asyncDecode(d);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.Transcoder#decode(net.spy.memcached.CachedData)
	 */
	public Object decode(CachedData d) {
		byte[] data=d.getData();
		Object rv=null;
		if((d.getFlags() & COMPRESSED) != 0) {
			data=decompress(d.getData());
		}
		int flags=d.getFlags() & SPECIAL_MASK;
		if((d.getFlags() & SERIALIZED) != 0 && data != null) {
			rv=deserialize(data);
		} else if((d.getFlags() & CACHE_ENTRY) != 0 && data != null) {
            rv=decodeCacheEntry(data);
		} else if(flags != 0 && data != null) {
			switch(flags) {
				case SPECIAL_BOOLEAN:
					rv=Boolean.valueOf(tu.decodeBoolean(data));
					break;
				case SPECIAL_INT:
					rv=new Integer(tu.decodeInt(data));
					break;
				case SPECIAL_LONG:
					rv=new Long(tu.decodeLong(data));
					break;
				case SPECIAL_DATE:
					rv=new Date(tu.decodeLong(data));
					break;
				case SPECIAL_BYTE:
					rv=new Byte(tu.decodeByte(data));
					break;
				case SPECIAL_FLOAT:
					rv=new Float(Float.intBitsToFloat(tu.decodeInt(data)));
					break;
				case SPECIAL_DOUBLE:
					rv=new Double(Double.longBitsToDouble(tu.decodeLong(data)));
					break;
				case SPECIAL_BYTEARRAY:
					rv=data;
					break;
				default:
					getLogger().warn("Undecodeable with flags %x", flags);
			}
		} else {
			rv=decodeString(data);
		}
		return rv;
	}


    /* (non-Javadoc)
      * @see net.spy.memcached.Transcoder#encode(java.lang.Object)
      */
	public CachedData encode(Object o) {
		byte[] b=null;
		int flags=0;
		if(o instanceof String) {
			b=encodeString((String)o);
		} else if(o instanceof Long) {
			b=tu.encodeLong((Long)o);
			flags |= SPECIAL_LONG;
		} else if(o instanceof Integer) {
			b=tu.encodeInt((Integer)o);
			flags |= SPECIAL_INT;
		} else if(o instanceof Boolean) {
			b=tu.encodeBoolean((Boolean)o);
			flags |= SPECIAL_BOOLEAN;
		} else if(o instanceof Date) {
			b=tu.encodeLong(((Date)o).getTime());
			flags |= SPECIAL_DATE;
		} else if(o instanceof Byte) {
			b=tu.encodeByte((Byte)o);
			flags |= SPECIAL_BYTE;
		} else if(o instanceof Float) {
			b=tu.encodeInt(Float.floatToRawIntBits((Float)o));
			flags |= SPECIAL_FLOAT;
		} else if(o instanceof Double) {
			b=tu.encodeLong(Double.doubleToRawLongBits((Double)o));
			flags |= SPECIAL_DOUBLE;
		} else if(o instanceof byte[]) {
			b=(byte[])o;
			flags |= SPECIAL_BYTEARRAY;
		} else if(o instanceof CacheEntry && WITH_CACHE_ENTRY) {
			b=encodeCacheEntry((CacheEntry)o);
			flags |= CACHE_ENTRY;
		} else {
			b=serialize(o);
			flags |= SERIALIZED;
		}
		assert b != null;
		if(b.length > compressionThreshold) {
			byte[] compressed=compress(b);
			if(compressed.length < b.length) {
				getLogger().debug("Compressed %s from %d to %d",
					o.getClass().getName(), b.length, compressed.length);
				b=compressed;
				flags |= COMPRESSED;
			} else {
				getLogger().info(
					"Compression increased the size of %s from %d to %d",
					o.getClass().getName(), b.length, compressed.length);
			}
		}
		return new CachedData(flags, b, getMaxSize());
	}

	public byte[] encodeCacheEntry(CacheEntry cacheEntry) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			cacheEntry.messagePack(new Packer(out));
		} catch (IOException e) {
			getLogger().error("encode error",e);
		}

		return out.toByteArray();
	}

	public CacheEntry decodeCacheEntry(byte[] data) {
		CacheEntry dst = new CacheEntry();

		ByteArrayInputStream in = new ByteArrayInputStream(data);
		try {
			dst.messageUnpack(new Unpacker(in));
		} catch (MessageTypeException e) {
			getLogger().error("decode error",e);
		} catch (IOException e) {
			getLogger().error("decode error",e);
		}

		return dst;
	}

    @Override
    protected byte[] compress(byte[] in) {
        byte[] compressed = null;
        try {
            compressed = Snappy.compress(in);
        } catch (SnappyException e) {
            getLogger().error("", e);
        }
        return compressed;
    }

    @Override
    protected byte[] decompress(byte[] in) {
        byte[] decompressed = null;
        try {
            decompressed = Snappy.uncompress(in);
        } catch (SnappyException e) {
            getLogger().error("", e);
        }
        return decompressed;

    }
}
