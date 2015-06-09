package com.nice;

import wd.RowKeyDistributorByHashPrefix;
import com.google.common.hash.Hashing;
import com.google.common.primitives.UnsignedInts;


public class OneByteMurmurHash extends RowKeyDistributorByHashPrefix.OneByteSimpleHash {

    private int modulo;

    public OneByteMurmurHash(int modulo) {
        super(modulo);
        this.modulo = modulo;
    }

    @Override
    public byte[] getHashPrefix(byte[] originalKey) {
        int hash = (int)(UnsignedInts.toLong(Hashing.murmur3_32().hashBytes(originalKey).asInt()) % this.modulo);
        return new byte[] {(byte) (hash % this.modulo)};
    }
}
