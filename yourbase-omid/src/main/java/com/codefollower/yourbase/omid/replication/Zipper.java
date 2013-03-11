package com.codefollower.yourbase.omid.replication;

import org.jboss.netty.buffer.ChannelBuffer;

import com.codefollower.yourbase.omid.tso.TSOMessage;
import com.codefollower.yourbase.omid.tso.messages.AbortedTransactionReport;
import com.codefollower.yourbase.omid.tso.messages.CleanedTransactionReport;
import com.codefollower.yourbase.omid.tso.messages.CommittedTransactionReport;

/**
 * Encodes replication messages efficiently.
 * 
 * 1 byte msgs
 * 
 * 00xx xxxx => commit report (commit diff = 1; startTS diff = xx xxxx)
 * 010x xxxx => half abort report (startTS diff = x xxxx)
 * 011x xxxx => full abort report (startTS diff = x xxxx)
 * 
 * 2 byte msgs
 * 
 * 10xx xxxx xxxx xxxx => commit report (ct diff = 1; st diff = xx...)
 * 
 * Remaining msgs
 * 
 * 11xx xxxx yyyy yyyy [yyyy yyyy ...] => header = 11xx xxxx; data = yyyy...
 * 
 */

public class Zipper {
    long lastStartTimestamp = 0;
    long lastCommitTimestamp = 0;

    long lastHalfAbortedTimestamp = 0;
    long lastFullAbortedTimestamp = 0;

    public ZipperState getZipperState() {
        return new ZipperState(lastStartTimestamp, lastCommitTimestamp, lastHalfAbortedTimestamp, lastFullAbortedTimestamp);
    }

    public void encodeCommit(ChannelBuffer buffer, long startTimestamp, long commitTimestamp) {
        long startDiff = startTimestamp - lastStartTimestamp;
        long commitDiff = commitTimestamp - lastCommitTimestamp;
        if (commitDiff == 1 && startDiff >= -32 && startDiff <= 31) {
            startDiff &= 0x3f;
            buffer.writeByte((byte) startDiff);
        } else if (commitDiff == 1 && startDiff >= -8192 && startDiff <= 8191) {
            byte high = (byte) 0x80;
            high |= (startDiff >> 8) & (byte) 0x3f;
            byte low = (byte) (startDiff & (byte) 0xff);
            buffer.writeByte(high);
            buffer.writeByte(low);
        } else if (commitDiff >= Byte.MIN_VALUE && commitDiff <= Byte.MAX_VALUE) {
            if (startDiff >= Byte.MIN_VALUE && startDiff <= Byte.MAX_VALUE) {
                buffer.writeByte(TSOMessage.CommittedTransactionReportByteByte);
                buffer.writeByte((byte) startDiff);
            } else if (startDiff >= Short.MIN_VALUE && startDiff <= Short.MAX_VALUE) {
                buffer.writeByte(TSOMessage.CommittedTransactionReportShortByte);
                buffer.writeShort((short) startDiff);
            } else if (startDiff >= Integer.MIN_VALUE && startDiff <= Integer.MAX_VALUE) {
                buffer.writeByte(TSOMessage.CommittedTransactionReportIntegerByte);
                buffer.writeInt((int) startDiff);
            } else {
                buffer.writeByte(TSOMessage.CommittedTransactionReportLongByte);
                buffer.writeLong((byte) startDiff);
            }
            buffer.writeByte((byte) commitDiff);
        } else if (commitDiff >= Short.MIN_VALUE && commitDiff <= Short.MAX_VALUE) {
            if (startDiff >= Byte.MIN_VALUE && startDiff <= Byte.MAX_VALUE) {
                buffer.writeByte(TSOMessage.CommittedTransactionReportByteShort);
                buffer.writeByte((byte) startDiff);
            } else if (startDiff >= Short.MIN_VALUE && startDiff <= Short.MAX_VALUE) {
                buffer.writeByte(TSOMessage.CommittedTransactionReportShortShort);
                buffer.writeShort((short) startDiff);
            } else if (startDiff >= Integer.MIN_VALUE && startDiff <= Integer.MAX_VALUE) {
                buffer.writeByte(TSOMessage.CommittedTransactionReportIntegerShort);
                buffer.writeInt((int) startDiff);
            } else {
                buffer.writeByte(TSOMessage.CommittedTransactionReportLongShort);
                buffer.writeLong((byte) startDiff);
            }
            buffer.writeShort((short) commitDiff);
        } else {
            buffer.writeByte(TSOMessage.CommittedTransactionReport);
            buffer.writeLong(startTimestamp);
            buffer.writeLong(commitTimestamp);
        }
        lastStartTimestamp = startTimestamp;
        lastCommitTimestamp = commitTimestamp;
    }

    public void encodeHalfAbort(ChannelBuffer buffer, long startTimestamp) {
        long diff = startTimestamp - lastHalfAbortedTimestamp;
        if (diff >= -16 && diff <= 15) {
            buffer.writeByte((byte) ((diff & 0x1f) | (0x40)));
        } else if (diff >= Byte.MIN_VALUE && diff <= Byte.MAX_VALUE) {
            buffer.writeByte(TSOMessage.AbortedTransactionReportByte);
            buffer.writeByte((byte) diff);
        } else {
            buffer.writeByte(TSOMessage.AbortedTransactionReport);
            buffer.writeLong(startTimestamp);
        }

        lastHalfAbortedTimestamp = startTimestamp;
    }

    public void encodeFullAbort(ChannelBuffer buffer, long startTimestamp) {
        long diff = startTimestamp - lastFullAbortedTimestamp;
        if (diff >= -16 && diff <= 15) {
            buffer.writeByte((byte) ((diff & 0x1f) | (0x60)));
        } else if (diff >= Byte.MIN_VALUE && diff <= Byte.MAX_VALUE) {
            buffer.writeByte(TSOMessage.CleanedTransactionReportByte);
            buffer.writeByte((byte) diff);
        } else {
            buffer.writeByte(TSOMessage.CleanedTransactionReport);
            buffer.writeLong(startTimestamp);
        }

        lastFullAbortedTimestamp = startTimestamp;
    }

    public void encodeLargestIncrease(ChannelBuffer buffer, long largestTimestamp) {
        buffer.writeByte(TSOMessage.LargestDeletedTimestampReport);
        buffer.writeLong(largestTimestamp);
    }

    public TSOMessage decodeMessage(ChannelBuffer buffer) {
        byte type = buffer.readByte();
        if ((type & 0xC0) == 0x00) { // 00xx xxxx
            return decodeCommittedTransactionReport(type, buffer);
        } else if ((type & 0xE0) == 0x40) { // 010x xxxx
            return decodeHalfAbort(type);
        } else if ((type & 0xE0) == 0x60) { // 011x xxxx
            return decodeFullAbort(type);
        } else if ((type & 0x40) == 0) { // 10xx xxxx [...]
            return decodeCommittedTransactionReport(type, buffer);
        } else if (type >= TSOMessage.CommittedTransactionReport) {
            return decodeCommittedTransactionReport(type, buffer);
        } else {
            switch (type) {
            case TSOMessage.CleanedTransactionReport:
            case TSOMessage.CleanedTransactionReportByte:
                return decodeFullAbort(type, buffer);
            case TSOMessage.AbortedTransactionReport:
            case TSOMessage.AbortedTransactionReportByte:
                return decodeHalfAbort(type, buffer);
            case TSOMessage.ZipperState:
                return decodeZipperState(buffer);
            default:
                return null;
            }
        }
    }

    private TSOMessage decodeZipperState(ChannelBuffer buffer) {
        ZipperState state = new ZipperState();
        state.readObject(buffer);
        this.lastCommitTimestamp = state.getLastCommitTimestamp();
        this.lastStartTimestamp = state.getLastStartTimestamp();
        this.lastFullAbortedTimestamp = state.getLastFullAbortedTimestamp();
        this.lastHalfAbortedTimestamp = state.getLastHalfAbortedTimestamp();
        return state;
    }

    private TSOMessage decodeHalfAbort(byte diff) {
        // Half abort
        lastHalfAbortedTimestamp += extractAbortedDifference(diff);
        return new AbortedTransactionReport(lastHalfAbortedTimestamp);
    }

    private TSOMessage decodeFullAbort(byte diff) {
        // Full abort
        lastFullAbortedTimestamp += extractAbortedDifference(diff);
        return new CleanedTransactionReport(lastFullAbortedTimestamp);
    }

    private int extractAbortedDifference(byte diff) {
        // extract the difference
        int extracted = diff & 0x1f;
        // extend the sign
        return (extracted << 27) >> 27;
    }

    private AbortedTransactionReport decodeHalfAbort(byte type, ChannelBuffer buffer) {
        AbortedTransactionReport msg;
        if (type == TSOMessage.AbortedTransactionReport) {
            msg = new AbortedTransactionReport();
            msg.readObject(buffer);
        } else {
            msg = new AbortedTransactionReport();
            int diff = buffer.readByte();
            msg.startTimestamp = lastHalfAbortedTimestamp + diff;
        }
        lastHalfAbortedTimestamp = msg.startTimestamp;

        return msg;
    }

    private CleanedTransactionReport decodeFullAbort(byte type, ChannelBuffer buffer) {
        CleanedTransactionReport msg;
        if (type == TSOMessage.CleanedTransactionReport) {
            msg = new CleanedTransactionReport();
            msg.readObject(buffer);
        } else {
            msg = new CleanedTransactionReport();
            int diff = buffer.readByte();
            msg.startTimestamp = lastFullAbortedTimestamp + diff;
        }
        lastFullAbortedTimestamp = msg.startTimestamp;

        return msg;
    }

    private CommittedTransactionReport decodeCommittedTransactionReport(byte high, ChannelBuffer aInputStream) {
        long startTimestamp = 0;
        long commitTimestamp = 0;
        if (high >= 0) {
            high = (byte) ((high << 26) >> 26);
            startTimestamp = lastStartTimestamp + high;
            commitTimestamp = lastCommitTimestamp + 1;
        } else if ((high & 0x40) == 0) {
            byte low = aInputStream.readByte();
            long startDiff = low & 0xff;
            startDiff |= ((high & 0x3f) << 26) >> 18;
            startTimestamp = lastStartTimestamp + startDiff;
            commitTimestamp = lastCommitTimestamp + 1;
        } else {
            switch (high) {
            case TSOMessage.CommittedTransactionReportByteByte:
                startTimestamp = lastStartTimestamp + aInputStream.readByte();
                commitTimestamp = lastCommitTimestamp + aInputStream.readByte();
                break;
            case TSOMessage.CommittedTransactionReportShortByte:
                startTimestamp = lastStartTimestamp + aInputStream.readShort();
                commitTimestamp = lastCommitTimestamp + aInputStream.readByte();
                break;
            case TSOMessage.CommittedTransactionReportIntegerByte:
                startTimestamp = lastStartTimestamp + aInputStream.readInt();
                commitTimestamp = lastCommitTimestamp + aInputStream.readByte();
                break;
            case TSOMessage.CommittedTransactionReportLongByte:
                startTimestamp = lastStartTimestamp + aInputStream.readLong();
                commitTimestamp = lastCommitTimestamp + aInputStream.readByte();
                break;

            case TSOMessage.CommittedTransactionReportByteShort:
                startTimestamp = lastStartTimestamp + aInputStream.readByte();
                commitTimestamp = lastCommitTimestamp + aInputStream.readShort();
                break;
            case TSOMessage.CommittedTransactionReportShortShort:
                startTimestamp = lastStartTimestamp + aInputStream.readShort();
                commitTimestamp = lastCommitTimestamp + aInputStream.readShort();
                break;
            case TSOMessage.CommittedTransactionReportIntegerShort:
                startTimestamp = lastStartTimestamp + aInputStream.readInt();
                commitTimestamp = lastCommitTimestamp + aInputStream.readShort();
                break;
            case TSOMessage.CommittedTransactionReportLongShort:
                startTimestamp = lastStartTimestamp + aInputStream.readLong();
                commitTimestamp = lastCommitTimestamp + aInputStream.readShort();
                break;
            case TSOMessage.CommittedTransactionReport:
                startTimestamp = aInputStream.readLong();
                commitTimestamp = aInputStream.readLong();
            }
        }

        lastStartTimestamp = startTimestamp;
        lastCommitTimestamp = commitTimestamp;

        return new CommittedTransactionReport(startTimestamp, commitTimestamp);
    }

}
